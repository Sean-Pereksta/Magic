'use strict';
// Dedicated service. Never starts/deploys from the static game page.
const express=require('express');const admin=require('firebase-admin');
const {createRouter}=require('./http.cjs');
const {PROJECT_ID}=require('./mount.cjs');
const projectId=process.env.GOOGLE_CLOUD_PROJECT||process.env.GCLOUD_PROJECT;
if(projectId&&projectId!==PROJECT_ID)throw new Error('Faction Forge must use the existing CatnMice Firebase project: '+PROJECT_ID);
if(!projectId)throw new Error('Set GOOGLE_CLOUD_PROJECT to the existing CatnMice Firebase project.');
if(!admin.apps.length)admin.initializeApp({projectId,credential:admin.credential.applicationDefault()});
const app=express();app.disable('x-powered-by');
const origins=(process.env.FACTION_FORGE_ALLOWED_ORIGINS||'').split(',').map(v=>v.trim()).filter(Boolean);
if(!origins.length)throw new Error('Set FACTION_FORGE_ALLOWED_ORIGINS to the exact game site origin(s).');
app.use('/api/faction-forge',createRouter({db:admin.firestore(),auth:admin.auth(),allowedOrigins:origins}));
const port=Number(process.env.PORT)||8788;app.listen(port,()=>console.log('Faction Forge progression service listening on '+port));
