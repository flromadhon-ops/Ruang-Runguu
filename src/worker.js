export class Room {
  constructor(state, env) {
    this.state = state;
  }

  async fetch(request) {
    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response("Butuh WebSocket", { status: 426 });
    }
    const url = new URL(request.url);
    const role = url.searchParams.get("role");
    const given = url.searchParams.get("token");
    if (role !== "staff" && role !== "guest") {
      return new Response("Peran tidak valid", { status: 400 });
    }

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    this.state.acceptWebSocket(server);

    const tokens = (await this.state.storage.get("tokens")) || [];
    let created = await this.state.storage.get("created");
    let token = given && tokens.includes(given) ? given : null;

    if (!token) {
      if (role === "staff") {
        for (const ws of this.state.getWebSockets()) {
          if (ws !== server) ws.close(1000, "sesi baru");
        }
        token = crypto.randomUUID();
        created = Date.now();
        await this.state.storage.put({ tokens: [token], created });
      } else {
        if (!created || Date.now() - created > 5 * 60 * 1000) {
          server.close(4001, "kedaluwarsa");
          return new Response(null, { status: 101, webSocket: client });
        }
        token = crypto.randomUUID();
        tokens.push(token);
        await this.state.storage.put("tokens", tokens);
      }
    }

    server.serializeAttachment({ role });
    server.send(JSON.stringify({ type: "hello", token }));
    this.broadcastPeers();
    return new Response(null, { status: 101, webSocket: client });
  }

  broadcastPeers(skip) {
    const sockets = this.state.getWebSockets().filter((w) => w !== skip);
    let staff = 0, guest = 0;
    for (const w of sockets) {
      const a = w.deserializeAttachment();
      if (a && a.role === "staff") staff++;
      else if (a && a.role === "guest") guest++;
    }
    const msg = JSON.stringify({ type: "peers", staff, guest });
    for (const w of sockets) w.send(msg);
  }

  webSocketMessage(ws, message) {
    let d;
    try { d = JSON.parse(message); } catch { return; }
    const a = ws.deserializeAttachment();
    if (!a || typeof d.t !== "string") return;
    const t = d.t.slice(0, 300);

    let toRole = null;
    if (a.role === "staff" && d.type === "msg") toRole = "guest";
    if (a.role === "guest" && d.type === "reply") toRole = "staff";
    if (!toRole) return;

    const out = JSON.stringify({ type: d.type, t });
    for (const w of this.state.getWebSockets()) {
      const x = w.deserializeAttachment();
      if (w !== ws && x && x.role === toRole) w.send(out);
    }
  }

  webSocketClose(ws) {
    this.broadcastPeers(ws);
  }

  webSocketError(ws) {
    this.broadcastPeers(ws);
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/ws") {
      const code = (url.searchParams.get("code") || "").replace(/\D/g, "");
      if (code.length !== 6) return new Response("Kode tidak valid", { status: 400 });
      return env.ROOM.get(env.ROOM.idFromName(code)).fetch(request);
    }
    return new Response("Not found", { status: 404 });
  },
};
