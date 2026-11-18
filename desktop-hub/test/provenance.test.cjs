const { test } = require('node:test');
const assert = require('node:assert/strict');
const { readFileSync } = require('node:fs');
const path = require('node:path');
const { verifyManifestProvenance, verifyRelease } = require('../desktop/provenance/verify.cjs');

// Real public Sigstore proof and artifact from cli/cli, commit
// e1219499a578b69e3437adaa31cd60614e23c802, pkg/cmd/attestation/test/data/
// reusable-workflow-{artifact,attestation.sigstore.json}. License beside fixtures.
const fixture = (name) => readFileSync(path.join(__dirname, 'fixtures/github-provenance', name));
const artifact = fixture('github-artifact.bin');
const bundle = fixture('github-bundle.json');
const policy = {
  subject: 'github_provenance_demo-0.0.0-py3-none-any.whl',
  signer: 'https://github.com/github/artifact-attestations-workflows/.github/workflows/attest.yml@09b495c3f12c7881b3cc17209a327792065c1a1d',
  repository: 'https://github.com/malancas/attest-demo',
  repositoryId: '804070735',
  sourceRef: 'refs/heads/main',
};

test('verifies a real GitHub build offline and rejects it for Catnmice', () => {
  verifyRelease(artifact, bundle, policy);
  assert.throws(() => verifyManifestProvenance(artifact, bundle));
});

test('binds the release bytes, exact signer, branch, repository name and immutable ID', () => {
  assert.throws(() => verifyRelease(Buffer.concat([artifact, Buffer.from('x')]), bundle, policy));
  for (const [field, value] of Object.entries(policy)) {
    assert.throws(() => verifyRelease(artifact, bundle, { ...policy, [field]: value + 'x' }), field);
  }
});

test('rejects a forged signature, changed statement or missing transparency evidence', () => {
  for (const change of [
    proof => { proof.dsseEnvelope.signatures[0].sig = Buffer.from('invalid').toString('base64'); },
    proof => { proof.dsseEnvelope.payload = Buffer.from('{}').toString('base64'); },
    proof => { proof.verificationMaterial.tlogEntries = []; },
  ]) {
    const proof = JSON.parse(bundle);
    change(proof);
    assert.throws(() => verifyRelease(artifact, Buffer.from(JSON.stringify(proof)), policy));
  }
});
