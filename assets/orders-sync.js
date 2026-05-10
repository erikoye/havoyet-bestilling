// Cross-tab order sync for bestillingssiden.
//
// Bruk:
//   1. <script src="assets/orders-sync.js"></script>  (én gang per side)
//   2. Etter en vellykket POST/PATCH/DELETE som endrer ordre:
//        window.HavoyetOrdersSync.notify();
//   3. Registrer reload-callback (typisk loadFromFlask / refresh / loadOrders):
//        window.HavoyetOrdersSync.onChange(loadFromFlask);
//
// Triggerene som kaller registrerte callbacks:
//   - BroadcastChannel('havoyet-orders') fra annen fane (samme origin)
//   - localStorage 'hv:orders-bump' (fallback for eldre nettlesere)
//   - tab blir synlig igjen (visibilitychange)
//   - vinduet får fokus (focus)
//
// Echo-guard: notify() lagrer en timestamp; broadcasts mottatt < 1 s etterpå
// ignoreres så fanen ikke re-fetcher umiddelbart etter sin egen mutasjon
// (lokal kode har allerede oppdatert UI-en — re-fetch ville bare bli støy).
(function(){
  var ECHO_GUARD_MS = 1000;
  var lastNotifyAt = 0;
  var callbacks = [];

  function fireFromBroadcast(){
    if (Date.now() - lastNotifyAt < ECHO_GUARD_MS) return;
    callbacks.forEach(function(cb){
      try { cb(); } catch (e) { console.warn('[orders-sync] callback feilet:', e); }
    });
  }

  function fireFromVisibility(){
    // Visibility/focus er aldri ekko av eget notify() — kjør alltid.
    callbacks.forEach(function(cb){
      try { cb(); } catch (e) { console.warn('[orders-sync] callback feilet:', e); }
    });
  }

  function notify(){
    lastNotifyAt = Date.now();
    try {
      if (typeof BroadcastChannel !== 'undefined') {
        var ch = new BroadcastChannel('havoyet-orders');
        ch.postMessage({ type: 'orders-changed', at: lastNotifyAt });
        ch.close();
      }
    } catch (e) {}
    try { localStorage.setItem('hv:orders-bump', String(lastNotifyAt)); } catch (e) {}
  }

  function onChange(cb){
    if (typeof cb === 'function') callbacks.push(cb);
  }

  try {
    if (typeof BroadcastChannel !== 'undefined') {
      var bc = new BroadcastChannel('havoyet-orders');
      bc.onmessage = function(ev){
        if (ev && ev.data && ev.data.type === 'orders-changed') fireFromBroadcast();
      };
    }
  } catch (e) {}

  window.addEventListener('storage', function(ev){
    if (ev && ev.key === 'hv:orders-bump') fireFromBroadcast();
  });

  document.addEventListener('visibilitychange', function(){
    if (document.visibilityState === 'visible') fireFromVisibility();
  });
  window.addEventListener('focus', fireFromVisibility);

  window.HavoyetOrdersSync = { notify: notify, onChange: onChange };
})();
