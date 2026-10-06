/* Run-scoped legacy timeouts. Reset/load/new battle invalidates stale callbacks. */
let ttTimerEpoch = 0;
const ttRunTimers = new Set();
function ttTimeout(callback, delay = 0) {
  const epoch = ttTimerEpoch;
  const id = setTimeout(() => {
    ttRunTimers.delete(id);
    if (epoch === ttTimerEpoch) callback();
  }, delay);
  ttRunTimers.add(id);
  return id;
}
function ttCancelTimeout(id) {
  clearTimeout(id);
  ttRunTimers.delete(id);
}
function ttClearTimers() {
  ttTimerEpoch++;
  ttRunTimers.forEach(id => clearTimeout(id));
  ttRunTimers.clear();
}
