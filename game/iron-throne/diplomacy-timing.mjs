// Each Council leader gets a full generation window after leaving the queue.
// The browser also allows time for Worker validation and network transit.
export function diplomacyTiming(mode) {
  return mode === 'allianceCouncil'
    ? { providerMs: 60000, clientMs: 75000 }
    : { providerMs: 12000, clientMs: 18000 };
}
