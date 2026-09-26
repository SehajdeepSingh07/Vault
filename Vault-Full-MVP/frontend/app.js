const $ = (s) => document.querySelector(s);
const state = { cluster:null, raft:null, health:null, repair:null, integrity:null, rebalance:null, faults:null, metadata:[] };

function log(msg) {
  const el = document.createElement('div');
  el.className = 'log-entry';
  const t = new Date().toLocaleTimeString();
  el.innerHTML = `<span class="time">${escapeHtml(t)}</span> <span class="msg">${escapeHtml(msg)}</span>`;
  $('#log').prepend(el);
}
function escapeHtml(s){return String(s).replace(/[&<>'"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;',"'":'&#39;','"':'&quot;'}[c]));}
function badge(el, text, kind=''){ el.textContent=text; el.className='badge'+(kind?` ${kind}`:''); }

async function json(path, options={}) { return VaultAPI.json(path, options); }
async function raw(path, options={}) { return VaultAPI.request(path, options); }

async function refresh(){
  try {
    const [health, cluster, raft, repair, integrity, rebalance, faults, metadata] = await Promise.all([
      json('/v1/health'), json('/v1/cluster'), json('/v1/raft'), json('/v1/repair'), json('/v1/integrity'), json('/v1/rebalance'),
      fetch(VaultAPI.url('/v1/faults')).then(r=>r.ok?r.json():null), json('/v1/metadata')
    ]);
    Object.assign(state,{health,cluster,raft,repair,integrity,rebalance,faults,metadata});
    render();
    $('#lastUpdated').textContent=new Date().toLocaleTimeString();
  } catch(err){ log(`Refresh failed: ${err.message}${err.requestId?` [${err.requestId}]`:''}`); }
}
function render(){
  const h=state.health||{}, c=state.cluster||{}, r=state.raft||{};
  $('#stats').innerHTML=[
    ['Nodes',`${c.healthy_nodes ?? 0}/${c.total_nodes ?? 0}`],['Healthy',c.healthy_nodes ?? 0],['Failed',c.failed_nodes ?? 0],
    ['RF',h.replication_factor ?? '—'],['Write Q',h.write_quorum ?? '—'],['Read Q',h.read_quorum ?? '—']
  ].map(([k,v])=>`<div class="stat"><div class="label">${k}</div><div class="value">${escapeHtml(v)}</div></div>`).join('');
  badge($('#clusterBadge'),c.degraded?'DEGRADED':'HEALTHY',c.degraded?'warn':'ok');
  badge($('#raftBadge'),r.status?.role||'UNKNOWN',r.status?.role==='LEADER'?'ok':'warn');
  badge($('#faultBadge'),state.faults?'enabled':'disabled',state.faults?'warn':'');
  $('#apiBase').textContent = VaultAPI.base;
  $('#raftPanel').innerHTML=[
    ['Role',r.status?.role],['Term',r.status?.term],['Leader',r.leader_id||'—'],['Leader address',r.leader_address||'—'],['Commit index',r.status?.commit_index],['Log length',r.status?.log_length]
  ].map(([k,v])=>`<div class="detail"><span class="k">${k}</span><span class="v">${escapeHtml(v ?? '—')}</span></div>`).join('');
  $('#clusterTable').innerHTML=`<table class="table"><thead><tr><th>Node</th><th>State</th><th>Missed</th><th>Epoch</th><th>Heartbeat age</th></tr></thead><tbody>${[{id:c.self?.id,status:c.self?.status,missed:c.self?.missed_heartbeats,epoch:c.self?.failure_epoch,age:c.self?.last_heartbeat_age_ms},...(c.peers||[])].map(n=>`<tr><td>${escapeHtml(n.id)}</td><td><span class="status"><span class="dot ${String(n.status||'').toLowerCase()}"></span>${escapeHtml(n.status||'—')}</span></td><td>${n.missed??0}</td><td>${n.epoch??0}</td><td>${n.age??0} ms</td></tr>`).join('')}</tbody></table>`;
  $('#recoveryPanel').innerHTML=[
    ['Repair runs',state.repair?.runs],['Objects repaired',state.repair?.objects_repaired],['Replicas repaired',state.repair?.replicas_repaired],['Repair errors',state.repair?.errors],
    ['Quarantines',state.integrity?.quarantines?.length||0],['Integrity checks',state.integrity?.checks||0],['Rebalance runs',state.rebalance?.runs],['Replicas moved',state.rebalance?.replicas_moved]
  ].map(([k,v])=>`<div class="detail"><span class="k">${k}</span><span class="v">${escapeHtml(v??0)}</span></div>`).join('');
  $('#rebalancePanel').innerHTML=[
    ['Runs',state.rebalance?.runs],['Objects scanned',state.rebalance?.objects_scanned],['Objects moved',state.rebalance?.objects_moved],['Replicas moved',state.rebalance?.replicas_moved],['Last success',state.rebalance?.last_success_at||'—']
  ].map(([k,v])=>`<div class="detail"><span class="k">${k}</span><span class="v">${escapeHtml(v??0)}</span></div>`).join('');
  const peers = [{id:c.self?.id},...(c.peers||[])];
  $('#faultPeer').innerHTML=peers.map(p=>`<option value="${escapeHtml(p.id)}">${escapeHtml(p.id)}</option>`).join('');
  $('#metadataTable').innerHTML = state.metadata.length ? `<table class="table"><thead><tr><th>Key</th><th>Version</th><th>Size</th><th>Checksum</th><th>Replicas</th></tr></thead><tbody>${state.metadata.map(m=>`<tr><td class="metadata-key">${escapeHtml(m.key)}</td><td>${m.version}</td><td>${m.size}</td><td class="small">${escapeHtml(m.sha256)}</td><td>${escapeHtml((m.replicas||[]).join(', '))}</td></tr>`).join('')}</tbody></table>` : '<div class="empty">No active metadata.</div>';
}

$('#refreshBtn').onclick=refresh;
$('#clearLogBtn').onclick=()=>$('#log').replaceChildren();
$('#uploadForm').onsubmit=async(e)=>{e.preventDefault(); const key=$('#objectKey').value.trim(), file=$('#objectFile').files[0]; if(!key||!file)return; try{const {response}=await raw(`/v1/objects/${encodeURIComponent(key)}`,{method:'PUT',headers:{'Content-Type':file.type||'application/octet-stream'},body:file}); $('#objectResult').textContent=`HTTP ${response.status}\nRequest: ${response.headers.get('X-Vault-Request-ID')||'—'}`; log(`PUT ${key} → ${response.status}`); await refresh();}catch(err){$('#objectResult').textContent=`${err.message}\nRequest: ${err.requestId||'—'}`;log(`PUT failed: ${err.message}`)}};
$('#headBtn').onclick=async()=>{const k=$('#lookupKey').value.trim(); if(!k)return; try{const {response}=await raw(`/v1/objects/${encodeURIComponent(k)}`,{method:'HEAD'}); const lines=[`HTTP ${response.status}`]; for(const n of ['Content-Length','ETag','X-Vault-Object-Version','X-Vault-Object-SHA256','X-Vault-Replicas','X-Vault-Read-Quorum','X-Vault-Read-Acks','X-Vault-Request-ID']){if(response.headers.get(n))lines.push(`${n}: ${response.headers.get(n)}`)} $('#objectResult').textContent=lines.join('\n'); log(`HEAD ${k} → ${response.status}`);}catch(err){$('#objectResult').textContent=`${err.message}\nRequest: ${err.requestId||'—'}`}};
$('#downloadBtn').onclick=()=>{const k=$('#lookupKey').value.trim();if(k)window.location=VaultAPI.url(`/v1/objects/${encodeURIComponent(k)}`)};
$('#deleteBtn').onclick=async()=>{const k=$('#lookupKey').value.trim();if(!k)return;try{const {response}=await raw(`/v1/objects/${encodeURIComponent(k)}`,{method:'DELETE'});$('#objectResult').textContent=`HTTP ${response.status}`;log(`DELETE ${k} → ${response.status}`);await refresh()}catch(err){log(`DELETE failed: ${err.message}`)}};
$('#repairBtn').onclick=async()=>{const k=$('#repairKey').value.trim();if(!k)return;try{const b=await json(`/v1/repair/${encodeURIComponent(k)}`,{method:'POST'});$('#repairResult').textContent=JSON.stringify(b,null,2);log(`Repair ${k} complete`);await refresh()}catch(err){$('#repairResult').textContent=err.message;log(`Repair failed: ${err.message}`)}};
$('#scrubBtn').onclick=async()=>{const k=$('#repairKey').value.trim();if(!k)return;try{const {response,body}=await raw(`/v1/integrity/${encodeURIComponent(k)}`,{method:'POST'});$('#repairResult').textContent=JSON.stringify(body,null,2);log(`Scrub ${k} → ${response.status}`);await refresh()}catch(err){$('#repairResult').textContent=err.message}};
$('#partitionBtn').onclick=async()=>{const id=$('#faultPeer').value;try{const b=await json('/v1/faults',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action:'partition',peer_id:id})});$('#faultResult').textContent=JSON.stringify(b,null,2);log(`Partition injected for ${id}`);await refresh()}catch(err){$('#faultResult').textContent=err.message}};
$('#clearFaultBtn').onclick=async()=>{try{const b=await json('/v1/faults',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action:'clear'})});$('#faultResult').textContent=JSON.stringify(b,null,2);log('Faults cleared');await refresh()}catch(err){$('#faultResult').textContent=err.message}};
$('#rebalanceBtn').onclick=async()=>{try{await json('/v1/rebalance',{method:'POST'});$('#rebalanceMsg').textContent='completed';log('Cluster rebalance completed');await refresh()}catch(err){$('#rebalanceMsg').textContent=err.message;log(`Rebalance failed: ${err.message}`)}};

refresh();
setInterval(refresh,5000);
