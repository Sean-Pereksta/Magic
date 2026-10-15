'use strict';
const fs = require('node:fs'),
  path = require('node:path');
const {
  initializeTestEnvironment,
  assertFails,
  assertSucceeds
} = require('@firebase/rules-unit-testing');
const {
  doc,
  setDoc,
  getDoc,
  updateDoc,
  deleteDoc,
  collection,
  getDocs,
  serverTimestamp
} = require('firebase/firestore');
(async () => {
  if (!process.env.FIRESTORE_EMULATOR_HOST)
    throw new Error(
      'Run with Firebase emulators:exec. Production Firebase is never used.'
    );
  const env = await initializeTestEnvironment({
    projectId: 'demo-turncraft',
    firestore: {
      rules: fs.readFileSync(
        process.env.TURNCRAFT_RULES_FILE || path.resolve(__dirname, '../../../firestore.rules'),
        'utf8'
      )
    }
  });
  try {
    await env.withSecurityRulesDisabled(async (context) => {
      const db = context.firestore();
      for (const [id, ranked] of [
        ['custom', false],
        ['ranked', true]
      ])
        await setDoc(doc(db, 'lobbies', id), {
          name: 'Rules fixture',
          createdAt: 0,
          updatedAt: 0,
          gameType: 'turncraft',
          turncraftVersion: 1,
          status: 'started',
          visibility: 'public',
          ranked,
          hostUid: 'owner',
          host: 'Owner',
          members: { owner: 'Owner', peer: 'Peer' },
          players: ['Owner', 'Peer'],
          turncraftSeats: ['owner', 'peer'],
          turncraftPlayers: {owner: {name:'Owner',factionId:'frontier',team:'ffa',ready:true,avatar:null},peer: {name:'Peer',factionId:'ascendant',team:'ffa',ready:true,avatar:null}},
          settings: {
            mapId: 'classic',
            gameMode: 'annihilation',
            maxPlayers: 2,
            startingResources: 'standard'
          },
          chessStarfrontController: {
            ownerUid: 'owner',
            ownerName: 'Owner',
            epoch: 1,
            expiresAtMs: Date.now() + 60000
          }
        });
      await setDoc(doc(db, 'lobbies', 'legacy-turncraft'), {
        gameType: 'turncraft',
        host: 'Legacy',
        players: ['Legacy', 'Peer'],
        status: 'started'
      });
      await setDoc(doc(db, 'lobbies', 'other'), {
        gameType: 'other',
        players: ['Owner'],
        host: 'Owner',
        status: 'waiting'
      });
      await setDoc(doc(db, 'users', 'Owner'), {
        gameStats: {
          turncraft: { mmr: 1500, rankedMatches: 10 },
          otherGame: { wins: 9 }
        },
        bio: 'Original'
      });
      await setDoc(doc(db, 'users', 'Unmigrated'), {
        gameStats: { otherGame: { wins: 4 } },
        bio: 'Legacy'
      });
      await setDoc(doc(db, 'factionForgeNames', 'Owner'), { uid: 'owner' });
      await setDoc(doc(db, 'turncraftProfiles', 'owner'), {
        uid: 'owner',
        mmr: 1500
      });
      await setDoc(doc(db, 'turncraftProfiles', 'owner', 'history', 'match'), {
        verified: true
      });
      await setDoc(doc(db, 'turncraftRankings', 'owner'), {
        uid: 'owner',
        mmr: 1500
      });
    });
    const owner = env.authenticatedContext('owner').firestore(),
      peer = env.authenticatedContext('peer').firestore(),
      stranger = env.authenticatedContext('stranger').firestore(),
      guest = env.unauthenticatedContext().firestore();
    await assertSucceeds(getDoc(doc(owner, 'turncraftProfiles', 'owner')));
    await assertSucceeds(
      getDoc(doc(owner, 'turncraftProfiles', 'owner', 'history', 'match'))
    );
    await assertFails(getDoc(doc(peer, 'turncraftProfiles', 'owner')));
    await assertFails(getDoc(doc(guest, 'turncraftProfiles', 'owner')));
    await assertFails(getDocs(collection(owner, 'turncraftProfiles')));
    await assertSucceeds(getDocs(collection(owner, 'turncraftRankings')));
    await assertFails(
      updateDoc(doc(owner, 'turncraftProfiles', 'owner'), { mmr: 9999 })
    );
    await assertFails(
      setDoc(doc(owner, 'turncraftProfiles', 'owner', 'history', 'fake'), {
        verified: true
      })
    );
    await assertFails(
      updateDoc(doc(owner, 'turncraftRankings', 'owner'), { mmr: 9999 })
    );
    for (const name of [
      'turncraftMatches',
      'turncraftInvites',
      'turncraftLobbySecrets',
      'turncraftLobbyRequests',
      'turncraftRatingPairs',
      'turncraftRankedQueue'
    ]) {
      await assertFails(
        setDoc(doc(owner, name, 'forged'), {
          uid: 'owner',
          winner: 'owner',
          verified: true
        })
      );
      if (name !== 'turncraftInvites') await assertFails(getDoc(doc(owner, name, 'forged')));
      else await assertSucceeds(getDoc(doc(owner, name, 'forged')));
    }
    await assertFails(
      updateDoc(doc(owner, 'users', 'Owner'), {
        'gameStats.turncraft.mmr': 9999
      })
    );
    await assertFails(
      updateDoc(doc(owner, 'users', 'Owner'), {
        gameStats: { otherGame: { wins: 100 } }
      })
    );
    await assertFails(
      updateDoc(doc(peer, 'users', 'Owner'), { bio: 'Hijacked' })
    );
    await assertSucceeds(
      updateDoc(doc(owner, 'users', 'Owner'), {
        'gameStats.otherGame.wins': 10,
        bio: 'Updated'
      })
    );
    await assertSucceeds(
      updateDoc(doc(stranger, 'users', 'Unmigrated'), {
        bio: 'Compatible legacy field',
        'gameStats.otherGame.wins': 5
      })
    );
    await assertFails(
      setDoc(doc(owner, 'users', 'New'), {
        gameStats: { turncraft: { mmr: 1000 } }
      })
    );
    await assertFails(
      setDoc(doc(owner, 'lobbies', 'forged-room'), {
        gameType: 'turncraft',
        turncraftVersion: 1,
        members: { owner: 'Owner' }
      })
    );
    await assertFails(
      updateDoc(doc(owner, 'lobbies', 'custom'), { status: 'waiting' })
    );
    await assertFails(
      updateDoc(doc(peer, 'lobbies', 'custom'), { hostUid: 'peer' })
    );
    await assertFails(
      updateDoc(doc(owner, 'lobbies', 'custom'), { ranked: true })
    );
    await assertFails(
      updateDoc(doc(owner, 'lobbies', 'custom'), { gameType: 'other' })
    );
    await assertFails(deleteDoc(doc(owner, 'lobbies', 'custom')));
    await assertSucceeds(
      updateDoc(doc(owner, 'lobbies', 'custom'), {
        chessStarfrontController: {
          ownerUid: 'owner',
          ownerName: 'Owner',
          epoch: 1,
          expiresAtMs: Date.now() + 16000
        },
        updatedAt: serverTimestamp()
      })
    );
    await assertFails(
      updateDoc(doc(peer, 'lobbies', 'custom'), {
        chessStarfrontController: {
          ownerUid: 'peer',
          ownerName: 'Peer',
          epoch: 2,
          expiresAtMs: Date.now() + 16000
        }
      })
    );
    await assertFails(
      updateDoc(doc(owner, 'lobbies', 'ranked'), {
        chessStarfrontController: {
          ownerUid: 'owner',
          ownerName: 'Owner',
          epoch: 1,
          expiresAtMs: Date.now() + 16000
        }
      })
    );
    const state = {
      gameKey: 'chess_starfront',
      controllerUid: 'owner',
      controllerEpoch: 1,
      players: [{name:'Owner',factionId:'frontier',ai:false},{name:'Peer',factionId:'ascendant',ai:false}],
      chosenMapId:'classic',boardWidth:42,boardHeight:30,terrainCode:'g'.repeat(42*30),
      entities: []
    };
    await assertSucceeds(
      setDoc(doc(owner, 'lobbies', 'custom', 'chess_starfront', 'state'), state)
    );
    await assertSucceeds(
      getDoc(doc(peer, 'lobbies', 'custom', 'chess_starfront', 'state'))
    );
    await assertFails(
      getDoc(doc(stranger, 'lobbies', 'custom', 'chess_starfront', 'state'))
    );
    await assertFails(
      setDoc(doc(peer, 'lobbies', 'custom', 'chess_starfront', 'state'), {
        ...state,
        controllerUid: 'peer'
      })
    );
    await assertFails(
      setDoc(doc(owner, 'lobbies', 'ranked', 'chess_starfront', 'state'), state)
    );
    await assertSucceeds(
      setDoc(
        doc(peer, 'lobbies', 'custom', 'chess_starfront_presence', 'Peer'),
        { uid: 'peer', username: 'Peer', slot: 1, lastSeenMs: Date.now() }
      )
    );
    await assertFails(
      setDoc(
        doc(peer, 'lobbies', 'custom', 'chess_starfront_presence', 'Owner'),
        { uid: 'owner', username: 'Owner', slot: 0 }
      )
    );
    await assertFails(
      setDoc(
        doc(peer, 'lobbies', 'custom', 'chess_starfront_presence', 'Owner'),
        { uid: 'peer', username: 'Owner', slot: 0 }
      )
    );
    await assertSucceeds(
      setDoc(
        doc(
          peer,
          'lobbies',
          'custom',
          'chess_starfront_commands',
          'own-command'
        ),
        { by: 'Peer', slot: 1, type: 'move' }
      )
    );
    await assertFails(
      setDoc(
        doc(
          peer,
          'lobbies',
          'custom',
          'chess_starfront_commands',
          'impersonate'
        ),
        { by: 'Owner', slot: 0, type: 'move' }
      )
    );
    await assertFails(
      setDoc(
        doc(
          stranger,
          'lobbies',
          'custom',
          'chess_starfront_commands',
          'stranger'
        ),
        { by: 'Peer', slot: 1, type: 'move' }
      )
    );
    await assertFails(
      setDoc(
        doc(owner, 'lobbies', 'ranked', 'chess_starfront_commands', 'forged'),
        { by: 'Owner', slot: 0, type: 'move' }
      )
    );
    await assertFails(
      deleteDoc(
        doc(
          peer,
          'lobbies',
          'custom',
          'chess_starfront_commands',
          'own-command'
        )
      )
    );
    await assertSucceeds(
      deleteDoc(
        doc(
          owner,
          'lobbies',
          'custom',
          'chess_starfront_commands',
          'own-command'
        )
      )
    );
    await assertFails(
      setDoc(doc(stranger, 'lobbies', 'custom', 'arbitrary', 'state'), {
        winner: 'stranger'
      })
    );
    await assertFails(
      setDoc(doc(stranger, 'lobbies', 'custom', 'crowncouncil', 'state'), {
        winner: 'stranger'
      })
    );
    await assertSucceeds(
      setDoc(
        doc(
          stranger,
          'lobbies',
          'legacy-turncraft',
          'chess_starfront',
          'state'
        ),
        { version: 1 }
      )
    );
    await assertSucceeds(
      updateDoc(doc(stranger, 'lobbies', 'legacy-turncraft'), {
        chessStarfrontReady: { Peer: { ready: true } }
      })
    );
    await assertFails(
      updateDoc(doc(stranger, 'lobbies', 'legacy-turncraft'), {
        turncraftVersion: 1,
        members: { stranger: 'Peer' }
      })
    );
    await assertSucceeds(
      setDoc(doc(stranger, 'lobbies', 'other', 'legacy-state', 'state'), {
        value: 1
      })
    );
    await assertSucceeds(
      updateDoc(doc(stranger, 'lobbies', 'other'), { status: 'started' })
    );
    console.log(
      'TurnCraft rules emulator: profile/MMR authority, UID lobby controls, private records, ranked isolation and legacy compatibility passed.'
    );
  } finally {
    await env.cleanup();
  }
})().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
