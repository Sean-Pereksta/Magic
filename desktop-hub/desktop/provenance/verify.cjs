const { createHash } = require('node:crypto');
const { readFileSync } = require('node:fs');
const { bundleFromJSON, isBundleWithDsseEnvelope } = require('@sigstore/bundle');
const { TrustedRoot } = require('@sigstore/protobuf-specs');
const { Verifier, toSignedEntity, toTrustMaterial } = require('@sigstore/verify');

// Public Good Instance trust material, from sigstore/root-signing at
// c6464baef4866a4787943a18f7edc3d4199e304b, targets/trusted_root.json.
// Ship root updates in a release already authenticated by an installed hub.
const roots = TrustedRoot.fromJSON(require('./trusted-root.json'));
const verifier = new Verifier(toTrustMaterial(roots), {
  ctlogThreshold: 1,
  tlogThreshold: 1,
  timestampThreshold: 1,
});
const issuer = 'https://token.actions.githubusercontent.com';
const repository = 'https://github.com/Sean-Pereksta/Magic';
const sourceRef = 'refs/heads/main';
const catnmicePolicy = {
  subject: 'github-stable.json',
  signer: repository + '/.github/workflows/catnmice-game-hub.yml@' + sourceRef,
  repository, sourceRef, repositoryId: '983681082',
};

// Fulcio encodes these identity fields as DER UTF8String certificate extensions.
// All expected field values here are short ASCII strings (DER short length).
function githubClaim(number, text) {
  const bytes = Buffer.from(text, 'utf8');
  if (bytes.length >= 128) throw Error('Unsupported release identity field.');
  return {
    oid: { id: [1, 3, 6, 1, 4, 1, 57264, 1, number] },
    value: Buffer.concat([Buffer.from([0x0c, bytes.length]), bytes]),
  };
}

function verifyRelease(bytes, bundleBytes, expected) {
  if (!(bytes instanceof Uint8Array) || !(bundleBytes instanceof Uint8Array) ||
      bytes.length === 0 || bytes.length > 1024 * 1024 ||
      bundleBytes.length === 0 || bundleBytes.length > 1024 * 1024) {
    throw Error('Invalid release manifest or provenance size.');
  }
  const bundle = bundleFromJSON(JSON.parse(Buffer.from(bundleBytes).toString('utf8')));
  if (!isBundleWithDsseEnvelope(bundle)) throw Error('Missing signed release statement.');
  const envelope = bundle.content.dsseEnvelope;
  if (envelope.payloadType !== 'application/vnd.in-toto+json') {
    throw Error('Unsupported signed release statement.');
  }

  const signer = verifier.verify(toSignedEntity(bundle), {
    extensions: { issuer },
    oids: [
      githubClaim(11, 'github-hosted'),
      githubClaim(12, expected.repository),
      githubClaim(14, expected.sourceRef),
      githubClaim(15, expected.repositoryId),
    ],
  });
  if (signer.identity?.subjectAlternativeName !== expected.signer) {
    throw Error('Unexpected GitHub release workflow.');
  }

  const statement = JSON.parse(Buffer.from(envelope.payload).toString('utf8'));
  const digest = createHash('sha256').update(bytes).digest('hex');
  if (statement._type !== 'https://in-toto.io/Statement/v1' ||
      statement.predicateType !== 'https://slsa.dev/provenance/v1' ||
      !Array.isArray(statement.subject) || statement.subject.length !== 1 ||
      statement.subject[0]?.name !== expected.subject ||
      statement.subject[0]?.digest?.sha256 !== digest) {
    throw Error('The release file does not match the verified GitHub build.');
  }
}

function verifyManifestProvenance(manifestBytes, bundleBytes) {
  verifyRelease(manifestBytes, bundleBytes, catnmicePolicy);
}

if (require.main === module) {
  try {
    const [manifest, proof] = process.argv.slice(2);
    verifyManifestProvenance(readFileSync(manifest), readFileSync(proof));
    console.log('Verified Catnmice release provenance.');
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}

module.exports = { verifyManifestProvenance, verifyRelease };
