// Read-only presentation helpers. Callers supply facts from the existing rules.
export function abilityReadiness({ blocked = '', used = false, stored = 0, required = 0 } = {}) {
  if (blocked) return { kind: 'unavailable', icon: '⊘', reason: blocked };
  if (used) return { kind: 'used', icon: '✓', reason: 'Already activated this turn.' };
  if (stored < required) return { kind: 'waiting', icon: '◷', missing: required - stored };
  return { kind: 'ready', icon: '◆', reason: 'Ready' };
}

export function comparePrintedForms(before, after) {
  return Object.keys({ ...before, ...after }).map(label => ({
    label, before: before[label] || 'None', after: after[label] || 'None',
    changed: (before[label] || 'None') !== (after[label] || 'None')
  })).filter(row => row.before !== 'None' || row.after !== 'None');
}

export function readinessSummary(rows) {
  if (!rows.length) return null;
  const ready = rows.filter(row => row.kind === 'ready').length;
  const first = rows.find(row => row.kind === 'unavailable') || rows.find(row => row.kind === 'waiting') || rows[0];
  return ready
    ? { kind: 'ready', icon: '◆', value: `${ready}/${rows.length}`, label: `${ready} of ${rows.length} abilities ready` }
    : { kind: first.kind, icon: first.icon, value: '', label: rows.map(row => `${row.label}: ${row.reason}`).join('; ') };
}
