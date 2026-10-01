// One scheduler per game client. Running requests finish; player messages go
// ahead of waiting background dispatches. No automatic provider retries.
export class DiplomacyQueue {
  pending = [];
  running = false;
  generation = 0;
  enqueue(run, { background = false, isCurrent = () => true, onStatus } = {}) {
    return new Promise((resolve, reject) => {
      const job = { run, background, isCurrent, onStatus, resolve, reject, generation: this.generation };
      this.pending.push(job);
      onStatus?.('queued');
      queueMicrotask(() => this.drain());
    });
  }
  cancel() {
    this.generation++;
    for (const job of this.pending.splice(0)) job.resolve(this.cancelled());
  }
  cancelled() { return { source: 'cancelled', responses: [], reply: '', intents: [], notice: 'Conversation cancelled because the campaign changed.' }; }
  async drain() {
    if (this.running) return;
    this.running = true;
    try {
      while (this.pending.length) {
        const index = this.pending.findIndex(job => !job.background);
        const job = this.pending.splice(index < 0 ? 0 : index, 1)[0];
        try {
          if (job.generation !== this.generation || !job.isCurrent()) { job.resolve(this.cancelled()); continue; }
          job.onStatus?.('sending');
          const response = await job.run();
          job.resolve(job.generation === this.generation && job.isCurrent() ? response : this.cancelled());
        } catch (error) { job.reject(error); }
      }
    } finally { this.running = false; }
  }
}
