// One scheduler per game client. Running requests finish; player messages go
// ahead of waiting background dispatches. Unready jobs remain queued without
// occupying the transport; wake() also runs after verification completes.
export class DiplomacyQueue {
  pending = [];
  running = false;
  generation = 0;
  timer = null;
  enqueue(run, { background = false, isCurrent = () => true, ready = () => true, onStatus } = {}) {
    return new Promise((resolve, reject) => {
      const job = { run, background, isCurrent, ready, onStatus, resolve, reject, generation: this.generation };
      this.pending.push(job);
      onStatus?.('queued');
      queueMicrotask(() => this.drain());
    });
  }
  cancel() {
    this.generation++;
    clearTimeout(this.timer); this.timer = null;
    for (const job of this.pending.splice(0)) job.resolve(this.cancelled());
  }
  cancelled() { return { source: 'cancelled', responses: [], reply: '', intents: [], notice: 'Conversation cancelled because the campaign changed.' }; }
  wake() { clearTimeout(this.timer); this.timer = null; return this.drain(); }
  async drain() {
    if (this.running) return;
    this.running = true;
    try {
      while (this.pending.length) {
        const available = [];
        for (const job of [...this.pending]) {
          if (job.generation !== this.generation || !job.isCurrent()) {
            this.pending.splice(this.pending.indexOf(job), 1); job.resolve(this.cancelled()); continue;
          }
          const status = job.ready();
          if (status === true) available.push(job);
          else if (job.status !== status) { job.status = status; job.onStatus?.(status); }
        }
        const job = available.find(job => !job.background) || available[0];
        if (!job) break;
        this.pending.splice(this.pending.indexOf(job), 1);
        try {
          if (job.generation !== this.generation || !job.isCurrent()) { job.resolve(this.cancelled()); continue; }
          job.onStatus?.('sending');
          const response = await job.run();
          if (job.generation !== this.generation || !job.isCurrent()) job.resolve(this.cancelled());
          else if (response?.source === 'deferred') { this.pending.unshift(job); job.status = null; }
          else job.resolve(response);
        } catch (error) { job.reject(error); }
      }
    } finally {
      this.running = false;
      if (this.pending.length && !this.timer) this.timer = setTimeout(() => this.wake(), 1000);
    }
  }
}
