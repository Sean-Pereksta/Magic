'use strict';
const fs = require('node:fs'),
  path = require('node:path');
const assert = require('node:assert/strict');
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
  serverTimestamp,
  writeBatch
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
    await env.clearFirestore();
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
            fillAi: false,
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
      await assertFails(getDoc(doc(owner, name, 'forged')));
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
    await assertSucceeds(
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
    await assertSucceeds(
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
    await assertSucceeds(
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
    // The finished lobby and participant identity, rather than a free-standing
    // browser winner claim, determine every atomic profile/history increment.
    const {pathToFileURL}=require('node:url');
    const {completeCustomMatch}=await import(pathToFileURL(path.resolve(__dirname,'../shattered-front/lobby-store.mjs')).href);
    const {saveMatchResult}=await import(pathToFileURL(path.resolve(__dirname,'../shattered-front/result-store.mjs')).href);
    const ownerAuth={currentUser:{uid:'owner'}};
    await assert.rejects(saveMatchResult({db:owner,auth:ownerAuth,api:require('firebase/firestore')},'ranked','Owner'),/still syncing/);
    await updateDoc(doc(owner,'lobbies','ranked'),{chessStarfrontController:{ownerUid:'owner',ownerName:'Owner',epoch:1,expiresAtMs:Date.now()+16000},updatedAt:serverTimestamp()});
    await setDoc(doc(owner,'lobbies','ranked','chess_starfront','state'),{...state,gameOver:true,matchStartedAt:Date.now()-120000,matchResult:{winnerIdx:0,reason:'Every opposing command base fell.',finishedAt:Date.now()}});
    const finished=await completeCustomMatch({db:owner,auth:ownerAuth,api:require('firebase/firestore')},'ranked');
    const receipt={id:'ranked',lobbyId:'ranked',uid:'owner',username:'Owner',mode:'ranked',outcome:'win',winnerIdx:0,factionId:'frontier',opponents:['Peer'],mapId:'classic',gameMode:'annihilation',durationSeconds:finished.turncraftResult.durationSeconds,completedAt:finished.finishedAt,mmrBefore:1500,mmrAfter:1525,mmrDelta:25};
    await assertFails(setDoc(doc(owner,'users','Owner','turncraftHistory','ranked'),receipt));
    const forged=writeBatch(owner);
    forged.set(doc(owner,'users','Owner','turncraftHistory','ranked'),{...receipt,mmrAfter:9999,mmrDelta:8499});
    forged.update(doc(owner,'users','Owner'),{'gameStats.turncraft.mmr':9999,'gameStats.turncraft.lastMatchId':'ranked','gameStats.turncraft.multiplayerMatches':1});
    await assertFails(forged.commit());
    await assertFails(setDoc(doc(stranger,'users','Stranger','turncraftHistory','ranked'),{...receipt,uid:'stranger',username:'Stranger'}));
    await assertFails(updateDoc(doc(owner,'lobbies','ranked'),{turncraftResult:{...finished.turncraftResult,winnerIdx:1}}));
    await assertFails(updateDoc(doc(peer,'lobbies','ranked'),{members:{owner:'Owner'},players:['Owner'],turncraftSeats:['owner'],turncraftPlayers:{owner:finished.turncraftPlayers.owner}}));
    await saveMatchResult({db:owner,auth:ownerAuth,api:require('firebase/firestore')},'ranked','Owner');
    assert.equal((await saveMatchResult({db:owner,auth:ownerAuth,api:require('firebase/firestore')},'ranked','Owner')).wrote,false);
    const savedProfile=(await getDoc(doc(owner,'users','Owner'))).data();
    assert.equal(savedProfile.gameStats.turncraft.mmr,1525);
    assert.equal(savedProfile.gameStats.turncraft.highestMMR,1525);
    assert.equal(savedProfile.gameStats.turncraft.rankedMatches,11);
    assert.equal(savedProfile.gameStats.otherGame.wins,10);
    await assertFails(updateDoc(doc(owner,'users','Owner','turncraftHistory','ranked'),{mmrDelta:500}));
    await assertFails(deleteDoc(doc(owner,'users','Owner','turncraftHistory','ranked')));
    await assertFails(updateDoc(doc(owner,'users','Owner'),{'gameStats.turncraft.mmr':1550,'gameStats.turncraft.rankedMatches':12}));
    // Preserve the deployed Gridbound migration contract while preventing the
    // generic lobby grant from reopening Firestore active-combat documents.
    const room=doc(owner,'lobbies','other','rpg_rooms','durable');
    const durable={persistentState:{chests:{opened:true}},persistentUpdatedAt:serverTimestamp(),activeCombatMigratedToRTDB:true,activeCombatDatabase:'realtime-database',activeCombatSchemaVersion:3,migratedAt:serverTimestamp()};
    await assertSucceeds(setDoc(room,durable));
    await assertSucceeds(updateDoc(room,{persistentState:{chests:{opened:false}},persistentUpdatedAt:serverTimestamp()}));
    await assertFails(setDoc(doc(owner,'lobbies','other','rpg_rooms','active'),{...durable,enemies:[{hp:100}]}));
    await assertFails(updateDoc(room,{activeCombatDatabase:'firestore'}));
    await assertFails(updateDoc(room,{enemies:[{hp:100}]}));
    await assertFails(setDoc(doc(owner,'lobbies','other','rpg_rooms','durable','enemies','one'),{hp:100}));
    await assertFails(getDoc(doc(owner,'lobbies','other','rpg_rooms','durable','enemies','one')));
    await assertFails(deleteDoc(room));
    const reward=doc(owner,'rewards','gridbound-reward');
    const rewardData={id:'gridbound-reward',uid:'peer',username:'Peer',realmId:'realm',roomId:'room',activeRoomKey:'realm/room',controllerUid:'owner',sourceAttackId:'attack',enemyId:'enemy',enemyType:'slime',eliteAffix:null,isBoss:false,xp:10,gold:5,restoredStolenGold:0,item:null,materials:{},relicId:null,claimed:false,createdAtMs:Date.now(),createdAt:serverTimestamp()};
    await assertSucceeds(setDoc(reward,rewardData));
    await assertFails(getDoc(reward));
    await assertSucceeds(getDoc(doc(peer,'rewards','gridbound-reward')));
    await assertFails(getDoc(doc(stranger,'rewards','gridbound-reward')));
    await assertFails(setDoc(doc(owner,'rewards','oversized'),{...rewardData,id:'oversized',xp:100001}));
    await assertFails(setDoc(doc(owner,'rewards','wrong-controller'),{...rewardData,id:'wrong-controller',controllerUid:'peer'}));
    await assertFails(updateDoc(reward,{claimed:true,claimedAt:serverTimestamp()}));
    await assertFails(updateDoc(doc(peer,'rewards','gridbound-reward'),{xp:100,claimed:true,claimedAt:serverTimestamp()}));
    await assertFails(deleteDoc(doc(peer,'rewards','gridbound-reward')));
    await assertSucceeds(updateDoc(doc(peer,'rewards','gridbound-reward'),{claimed:true,claimedAt:serverTimestamp()}));
    await assertSucceeds(deleteDoc(doc(peer,'rewards','gridbound-reward')));
    await assertSucceeds(setDoc(doc(owner,'users','Unmigrated','otherGame','save'),{round:8}));
    await assertSucceeds(setDoc(doc(owner,'users','Unmigrated','otherGame','save','history','one'),{round:7}));
    await assertSucceeds(setDoc(doc(owner,'lobbies','other','invites','ordinary-game-invite'),{invite:'compatible'}));
    await assertSucceeds(setDoc(doc(owner,'lobbyInvites','ordinary-hub-invite'),{username:'Owner',lobbyId:'other'}));
    await assertSucceeds(getDoc(doc(peer,'lobbyInvites','ordinary-hub-invite')));
    await assertSucceeds(deleteDoc(doc(owner,'lobbyInvites','ordinary-hub-invite')));
    await assertFails(setDoc(doc(owner,'users','Owner','factionForge','profile'),{forgeDust:-1}));
    console.log(
      'TurnCraft/shared rules emulator: participant-only atomic rating/history receipts, immutable finished results, ranked gameplay, private records, Gridbound durable rooms/reward claims, legacy game saves and compatibility passed.'
    );
  } finally {
    await env.cleanup();
  }
})().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
