// Ruang Rungu: satu "ruang" (Durable Object) per kode 6 angka, dihubungkan lewat WebSocket.
const TTL_MS = 5 * 60 * 1000; // masa berlaku kode: 5 menit
const CLEANUP_MS = 12 * 60 * 60 * 1000;

export class Room {
  constructor(state) { this.state = state; }

  async fetch(req) {
    const q = new URL(req.url).searchParams;
    const role = q.get("role"), tok = q.get("token");
    if (req.headers.get("Upgrade") !== "websocket") return new Response("WebSocket only", { status: 426 });
    if (role !== "staff" && role !== "guest") return new Response("bad role", { status: 400 });
    const [client, server] = Object.values(new WebSocketPair());
    const st = this.state.storage;

    // Kode berlaku 5 menit sejak staf pertama kali mulai. Perangkat yang sudah
    // tersambung boleh menyambung ulang (pakai token) kalau koneksinya putus.
    let created = await st.get("created");
    const tokens = (await st.get("tokens")) || [];
    const known = !!tok && tokens.includes(tok);
    if (!created && role === "staff") {
      created = Date.now();
      await st.put("created", created);
      await st.setAlarm(created + CLEANUP_MS);
    }
    if (!created || (!known && Date.now() - created > TTL_MS)) {
      server.accept();
      server.close(4001, "expired");
      return new Response(null, { status: 101, webSocket: client });
    }
    let token = tok;
    if (!known) {
      token = crypto.randomUUID();
      tokens.push(token);
      await st.put("tokens", tokens);
    }

    this.state.acceptWebSocket(server);
    server.serializeAttachment({ role });
    server.send(JSON.stringify({ type: "hello", token }));
    if (role === "guest") {
      const last = await st.get("last"); // tamu yang baru masuk langsung dapat pesan terakhir
      if (last) server.send(JSON.stringify({ type: "msg", t: last }));
    }
    this.peers();
    return new Response(null, { status: 101, webSocket: client });
  }

  async alarm() { await this.state.storage.deleteAll(); } // bersihkan data ruang setelah 12 jam

  roleOf(ws) { return ws.deserializeAttachment()?.role; }

  async webSocketMessage(ws, raw) {
    let d; try { d = JSON.parse(raw); } catch { return; }
    const t = typeof d.t === "string" ? d.t.slice(0, 300) : "";
    if (!t) return;
    const role = this.roleOf(ws);
    if (role === "staff" && d.type === "msg") {
      await this.state.storage.put("last", t);
      this.send("guest", { type: "msg", t });
    } else if (role === "guest" && d.type === "reply") {
      this.send("staff", { type: "reply", t });
    }
  }

  webSocketClose(ws) { try { ws.close(); } catch {} this.peers(ws); }
  webSocketError(ws) { this.peers(ws); }

  send(role, obj) {
    const s = JSON.stringify(obj);
    for (const w of this.state.getWebSockets()) if (this.roleOf(w) === role) try { w.send(s); } catch {}
  }

  peers(exclude) {
    const n = { staff: 0, guest: 0 };
    const all = this.state.getWebSockets().filter((w) => w !== exclude);
    for (const w of all) n[this.roleOf(w)]++;
    const s = JSON.stringify({ type: "peers", ...n });
    for (const w of all) try { w.send(s); } catch {}
  }
}

export default {
  async fetch(req, env) {
    const u = new URL(req.url);
    if (u.pathname === "/ws") {
      const code = u.searchParams.get("code") || "";
      if (!/^\d{6}$/.test(code)) return new Response("bad code", { status: 400 });
      return env.ROOM.get(env.ROOM.idFromName(code)).fetch(req);
    }
    return new Response("Not found", { status: 404 }); // file statis dilayani otomatis dari ./public
  },
};
