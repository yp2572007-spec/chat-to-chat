/**
 * WebTalk Signalling Server
 * ─────────────────────────
 * • WebSocket server on port 3000  → chat + WebRTC signalling
 * • HTTP server on port 3001        → live monitor page (see all rooms/users)
 *
 * Install:  npm install ws
 * Run:      node server.js
 */

const http   = require('http');
const https  = require('https');
const { WebSocketServer, WebSocket } = require('ws');

const WS_PORT      = 3000;
const MONITOR_PORT = 3001;

// ─── State ───────────────────────────────────────────────────────────────────
// rooms  : { roomId: { users: Map<ws, {name,presKey,inCall,joinedAt}> } }
const rooms = new Map();

function getRoom(id) {
  if (!rooms.has(id)) rooms.set(id, { users: new Map() });
  return rooms.get(id);
}

function roomSnapshot(roomId) {
  const room = rooms.get(roomId);
  if (!room) return null;
  return {
    roomId,
    users: [...room.users.values()].map(u => ({
      name: u.name,
      presKey: u.presKey,
      inCall: u.inCall,
      joinedAt: u.joinedAt
    }))
  };
}

function broadcast(roomId, msg, excludeWs = null) {
  const room = rooms.get(roomId);
  if (!room) return;
  const payload = JSON.stringify(msg);
  for (const [ws] of room.users) {
    if (ws !== excludeWs && ws.readyState === WebSocket.OPEN) {
      ws.send(payload);
    }
  }
}

function broadcastPresence(roomId) {
  const snap = roomSnapshot(roomId);
  if (!snap) return;
  broadcast(roomId, { type: 'presence', room: snap });
  pushMonitorUpdate();
}

// ─── WebSocket Server (port 3000) ────────────────────────────────────────────
const wss = new WebSocketServer({ port: WS_PORT });
console.log(`[WebTalk] Signalling WS  → ws://localhost:${WS_PORT}`);

wss.on('connection', (ws, req) => {
  ws._roomId  = null;
  ws._name    = null;
  ws._presKey = null;

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    switch (msg.type) {

      // ── JOIN ROOM (chat) ──────────────────────────────────────────────────
      case 'join': {
        const { roomId, name, presKey } = msg;
        if (!roomId || !name) return;

        // leave old room if switching
        if (ws._roomId && ws._roomId !== roomId) leaveRoom(ws);

        ws._roomId  = roomId;
        ws._name    = name;
        ws._presKey = presKey || name;

        const room = getRoom(roomId);
        room.users.set(ws, {
          name,
          presKey: ws._presKey,
          inCall: false,
          joinedAt: Date.now()
        });

        // send full history + presence to the new user
        ws.send(JSON.stringify({ type: 'joined', roomId, name }));

        // tell everyone else
        broadcast(roomId, {
          type: 'sys',
          text: `${name} joined the room`,
          ts: Date.now()
        }, ws);

        broadcastPresence(roomId);
        break;
      }

      // ── CHAT MESSAGE ─────────────────────────────────────────────────────
      case 'msg': {
        if (!ws._roomId) return;
        const out = {
          type: 'msg',
          from: ws._name,
          presKey: ws._presKey,
          text: msg.text,
          ts: Date.now(),
          id: Math.random().toString(36).slice(2)
        };
        broadcast(ws._roomId, out); // send to ALL including sender (echo)
        break;
      }

      // ── SYS MESSAGE (file share, reactions, etc.) ─────────────────────
      case 'sys': {
        if (!ws._roomId) return;
        broadcast(ws._roomId, { type: 'sys', text: msg.text, ts: Date.now() }, ws);
        break;
      }

      // ── TYPING ────────────────────────────────────────────────────────────
      case 'typing': {
        if (!ws._roomId) return;
        broadcast(ws._roomId, {
          type: 'typing',
          name: ws._name,
          active: !!msg.active
        }, ws);
        break;
      }

      // ── CALL: JOIN ────────────────────────────────────────────────────────
      case 'call-join': {
        if (!ws._roomId) return;
        const u = rooms.get(ws._roomId)?.users.get(ws);
        if (u) u.inCall = true;
        broadcast(ws._roomId, {
          type: 'call-join',
          from: ws._name,
          pk: ws._presKey,
          ts: Date.now()
        }, ws);
        broadcastPresence(ws._roomId);
        break;
      }

      // ── CALL: LEAVE ───────────────────────────────────────────────────────
      case 'call-leave': {
        if (!ws._roomId) return;
        const u = rooms.get(ws._roomId)?.users.get(ws);
        if (u) u.inCall = false;
        broadcast(ws._roomId, {
          type: 'call-leave',
          from: ws._name,
          pk: ws._presKey,
          ts: Date.now()
        }, ws);
        broadcastPresence(ws._roomId);
        break;
      }

      // ── WebRTC SIGNALLING (offer / answer / ice) ─────────────────────────
      // Route to the named recipient in the same room
      case 'offer':
      case 'answer':
      case 'ice': {
        if (!ws._roomId) return;
        const room = rooms.get(ws._roomId);
        if (!room) return;
        // find target ws by name
        for (const [tws, tu] of room.users) {
          if (tu.name === msg.to && tws.readyState === WebSocket.OPEN) {
            tws.send(JSON.stringify({
              ...msg,
              from: ws._name,
              pk: ws._presKey
            }));
            break;
          }
        }
        break;
      }

      // ── PING ──────────────────────────────────────────────────────────────
      case 'ping': {
        ws.send(JSON.stringify({ type: 'pong', ts: Date.now() }));
        break;
      }
    }
  });

  ws.on('close', () => leaveRoom(ws));
  ws.on('error', () => leaveRoom(ws));
});

function leaveRoom(ws) {
  const { _roomId: roomId, _name: name } = ws;
  if (!roomId || !rooms.has(roomId)) return;
  const room = rooms.get(roomId);
  room.users.delete(ws);
  if (room.users.size === 0) {
    rooms.delete(roomId);
  } else {
    broadcast(roomId, { type: 'sys', text: `${name} left the room`, ts: Date.now() });
    broadcastPresence(roomId);
  }
  pushMonitorUpdate();
}

// ─── Monitor HTTP server (port 3001) ─────────────────────────────────────────
// Serves a live-updating dashboard page + SSE stream

const monitorClients = new Set();

function pushMonitorUpdate() {
  if (monitorClients.size === 0) return;
  const data = JSON.stringify(getMonitorData());
  for (const res of monitorClients) {
    try { res.write(`data: ${data}\n\n`); } catch {}
  }
}

function getMonitorData() {
  const result = [];
  for (const [roomId] of rooms) {
    const snap = roomSnapshot(roomId);
    if (snap) result.push(snap);
  }
  return {
    rooms: result,
    totalUsers: result.reduce((s, r) => s + r.users.length, 0),
    totalInCall: result.reduce((s, r) => s + r.users.filter(u => u.inCall).length, 0),
    ts: Date.now()
  };
}

const MONITOR_HTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>WebTalk Monitor</title>
<style>
  *{box-sizing:border-box;margin:0;padding:0}
  :root{
    --bg:#0f0f18;--surface:#1a1a2e;--surface2:#22223a;--border:#2e2e4a;
    --text:#e2e2f0;--text2:#9494b8;--accent:#7c6af7;--green:#22c55e;--red:#ef4444;--yellow:#f59e0b;
  }
  body{background:var(--bg);color:var(--text);font-family:'Segoe UI',system-ui,sans-serif;min-height:100dvh;}
  header{
    background:var(--surface);border-bottom:1px solid var(--border);
    padding:.8rem 1.4rem;display:flex;align-items:center;gap:.8rem;
  }
  header h1{font-size:1.05rem;font-weight:700;}
  .live-dot{width:9px;height:9px;border-radius:50%;background:var(--green);animation:blink 1.2s infinite;flex-shrink:0}
  @keyframes blink{0%,100%{opacity:1}50%{opacity:.3}}
  .stats{display:flex;gap:.6rem;margin-left:auto;}
  .stat{background:var(--surface2);border:1px solid var(--border);border-radius:8px;padding:.3rem .75rem;font-size:.72rem;font-weight:600;color:var(--text2);}
  .stat span{color:var(--text);font-size:.9rem;}
  main{padding:1rem 1.2rem;display:flex;flex-direction:column;gap:.8rem;}
  .empty{text-align:center;padding:4rem;color:var(--text2);font-size:.9rem;}
  .room-card{background:var(--surface);border:1px solid var(--border);border-radius:14px;overflow:hidden;}
  .room-header{
    display:flex;align-items:center;gap:.6rem;padding:.65rem 1rem;
    border-bottom:1px solid var(--border);background:var(--surface2);
  }
  .room-id{font-size:.85rem;font-weight:700;}
  .room-count{font-size:.7rem;background:var(--border);border-radius:2rem;padding:.1rem .5rem;color:var(--text2);}
  .call-badge{font-size:.68rem;background:rgba(34,197,94,.15);border:1px solid rgba(34,197,94,.3);color:var(--green);border-radius:2rem;padding:.1rem .5rem;}
  .user-list{display:flex;flex-direction:column;}
  .user-row{display:flex;align-items:center;gap:.7rem;padding:.55rem 1rem;border-bottom:1px solid var(--border);}
  .user-row:last-child{border-bottom:none;}
  .u-av{
    width:32px;height:32px;border-radius:50%;display:flex;align-items:center;
    justify-content:center;font-size:13px;font-weight:700;color:#fff;flex-shrink:0;
  }
  .u-name{font-size:.82rem;font-weight:600;flex:1;}
  .u-pk{font-size:.65rem;color:var(--text2);font-family:monospace;}
  .badge{font-size:.65rem;border-radius:2rem;padding:.12rem .45rem;font-weight:600;}
  .badge.call{background:rgba(34,197,94,.15);color:var(--green);border:1px solid rgba(34,197,94,.3);}
  .badge.chat{background:rgba(124,106,247,.15);color:var(--accent);border:1px solid rgba(124,106,247,.3);}
  .u-since{font-size:.62rem;color:var(--text2);}
  .ts{font-size:.7rem;color:var(--text2);text-align:right;padding:.4rem 1rem;border-top:1px solid var(--border);}
  .colors{background:linear-gradient(135deg,#7c6af7,#06b6d4,#22c55e);}
  /* avatar colours */
  .av0{background:#7c6af7}.av1{background:#06b6d4}.av2{background:#22c55e}.av3{background:#f59e0b}
  .av4{background:#ef4444}.av5{background:#ec4899}.av6{background:#8b5cf6}.av7{background:#14b8a6}
  .av8{background:#f97316}.av9{background:#6366f1}
</style>
</head>
<body>
<header>
  <span class="live-dot"></span>
  <h1>📡 WebTalk Monitor</h1>
  <div class="stats">
    <div class="stat">Rooms: <span id="s-rooms">0</span></div>
    <div class="stat">Online: <span id="s-users">0</span></div>
    <div class="stat">In Call: <span id="s-call">0</span></div>
    <div class="stat">Updated: <span id="s-ts">—</span></div>
  </div>
</header>
<main id="main"><div class="empty">No active rooms yet. Open the chat app to get started.</div></main>

<script>
function avColor(n){var s=0;for(var i=0;i<(n||'').length;i++)s+=n.charCodeAt(i);return s%10;}
function avInit(n){return(n||'?').slice(0,2).toUpperCase();}
function timeAgo(ts){
  var s=Math.floor((Date.now()-ts)/1000);
  if(s<60)return s+'s ago';
  if(s<3600)return Math.floor(s/60)+'m ago';
  return Math.floor(s/3600)+'h ago';
}

var es=new EventSource('/events');
es.onmessage=function(e){
  var d=JSON.parse(e.data);
  render(d);
};

function render(d){
  document.getElementById('s-rooms').textContent=d.rooms.length;
  document.getElementById('s-users').textContent=d.totalUsers;
  document.getElementById('s-call').textContent=d.totalInCall;
  document.getElementById('s-ts').textContent=new Date(d.ts).toLocaleTimeString();

  var main=document.getElementById('main');
  if(!d.rooms.length){main.innerHTML='<div class="empty">No active rooms. Open the chat app to get started.</div>';return;}

  main.innerHTML=d.rooms.map(function(r){
    var inCall=r.users.filter(function(u){return u.inCall;}).length;
    return '<div class="room-card">'
      +'<div class="room-header">'
      +'<div class="room-id"># '+r.roomId+'</div>'
      +'<div class="room-count">'+r.users.length+' member'+(r.users.length!==1?'s':'')+'</div>'
      +(inCall?'<div class="call-badge">📹 '+inCall+' in call</div>':'')
      +'</div>'
      +'<div class="user-list">'
      +r.users.map(function(u){
        var ci=avColor(u.name);
        return '<div class="user-row">'
          +'<div class="u-av av'+ci+'">'+avInit(u.name)+'</div>'
          +'<div><div class="u-name">'+u.name+'</div><div class="u-pk">'+u.presKey+'</div></div>'
          +'<span class="badge '+(u.inCall?'call':'chat')+'">'+(u.inCall?'📹 In Call':'💬 Chatting')+'</span>'
          +'<div class="u-since">'+timeAgo(u.joinedAt)+'</div>'
          +'</div>';
      }).join('')
      +'</div>'
      +'</div>';
  }).join('');
}

// Also poll as fallback every 10s
setInterval(function(){
  fetch('/data').then(function(r){return r.json();}).then(render).catch(function(){});
},10000);
</script>
</body>
</html>`;

const monitorServer = http.createServer((req, res) => {
  if (req.url === '/events') {
    // SSE stream
    res.writeHead(200, {
      'Content-Type':  'text/event-stream',
      'Cache-Control': 'no-cache',
      'Connection':    'keep-alive',
      'Access-Control-Allow-Origin': '*'
    });
    res.write(`data: ${JSON.stringify(getMonitorData())}\n\n`);
    monitorClients.add(res);
    req.on('close', () => monitorClients.delete(res));
    return;
  }
  if (req.url === '/data') {
    res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
    res.end(JSON.stringify(getMonitorData()));
    return;
  }
  // Serve monitor page
  res.writeHead(200, { 'Content-Type': 'text/html' });
  res.end(MONITOR_HTML);
});

monitorServer.listen(MONITOR_PORT, () => {
  console.log(`[WebTalk] Monitor page   → http://localhost:${MONITOR_PORT}`);
  console.log(`[WebTalk] Ready. Open the chat HTML and point WS_URL to ws://localhost:${WS_PORT}`);
});

// ─── Cleanup: remove empty/stale rooms every 5 min ───────────────────────────
setInterval(() => {
  for (const [id, room] of rooms) {
    if (room.users.size === 0) rooms.delete(id);
  }
}, 5 * 60 * 1000);
