import {FORGE_GEAR,GEAR_RARITIES} from './equipment-data.mjs';
import {LIVING_EQUIPMENT_OUTLINES} from './living-equipment-icons.mjs';

// Small original vector illustrations. Every new definition has its own outline;
// no network requests or bitmap downloads are needed as the collection grows.
const outlines={
 bambooKnuckles:'M13 37V22l7-5 5 5 7-6 7 5 6-2 7 7v17H13z M20 24v10m12-11v11m12-9v9',
 pebbleGuard:'M32 8 53 19l-4 25-17 13-17-13-4-25z M23 23l12-5 8 10-4 13-13 2-8-10z',
 emberFang:'M20 10c31-3 38 19 19 44l-9-17-13-9z M24 16l12 13',
 breezeRibbon:'M14 12h32L35 30l16 23-18-6-8-16-12 16 2-21z',
 riverglassShell:'M10 43C5 12 55 5 55 43L32 55z M32 17v31M21 21l7 27m16-27-8 27',
 frostButton:'M32 8a24 24 0 1 1 0 48 24 24 0 1 1 0-48z M24 24v2m16-2v2M24 39v2m16-2v2M32 10v7m0 30v7M10 32h7m30 0h7',
 duskBand:'M12 23q20-19 40 0v22q-20 17-40 0z M13 27q19 14 38 0M24 31l8 14 8-14',
 sunseedPendant:'M15 8q17 18 34 0M32 20c-28 18-17 37 0 35 17 2 28-17 0-35z M32 29v17m-7-8h14',
 foragersPouch:'M17 13h30l-5 12q19 26-10 31Q3 51 22 25z M17 26h30m-23-10h16M27 36q12-6 14 6-11 13-14-6z',
 trackersKnot:'M13 17q16-12 24 4 19-2 15 16-10 17-24 4-16 8-19-7z M15 28l34 3M26 17l9 28m-9-4-5 16m18-15 5 13',
 springbudLocket:'M16 8q16 20 32 0M32 23c-23-14-32 16 0 33 32-17 23-47 0-33z M32 43q-15-14-11-16 14-3 11 16 16-16 15-13 5 11-15 13',
 rootstoneCuffs:'M10 22 27 15v32L10 42zM37 15l17 7v20l-17 5z M11 31l15 5m12-9 14 8m-41 3 15-1m13 5 14-3',
 cinderOpener:'M11 40l28-28 14 14-28 28z M22 31l11 11m3-26 12 12M13 45l6 6m17-10 10 9 7-7-9-10',
 stormstepAnklet:'M15 14h31l-4 14 8 10-10 8-6-13-8 15-9-6 4-17z M31 12l-7 19h10l-6 22',
 tidecallShell:'M10 37Q5 5 37 12q33 15 9 40H22z M13 35l30 12M21 20l19 23m-7-28 9 26',
 rimeScarf:'M10 16h44v13L39 35l12 17-13 4-15-22-13-6z M10 22h43M39 47l6-3m-11-2 6-3',
 duskglassLens:'M13 14h29v29H13z M18 19h19v19H18zM39 40l14 14M19 33l17-11',
 dawnthreadBracelet:'M32 11a21 21 0 1 1 0 42 21 21 0 1 1 0-42z M32 18a14 14 0 1 0 0 28 14 14 0 1 0 0-28z M44 12l8 7-8 8-8-8z',
 campwalkerBrooch:'M9 44 24 15l8 12 8-18 15 35z M21 44V29m19 15V25M16 49h34m-16-6v9',
 fruitkeeperClip:'M15 10h35v13H28v24q-12 17-19-1V30h11v14 M34 28q17-8 18 8-1 17-14 11-15-9-4-19z',
 briarloopBand:'M32 13a19 19 0 1 1 0 38 19 19 0 1 1 0-38z M17 19l-9-6 4 17m33-11 10-5-4 17M19 47l-7 7 17-4m17-3 8 8-17-4',
 quakehideBelt:'M8 24q24 8 48 0v19q-24 8-48 0z M23 20h19v28H23z M32 24l-4 9 9 3-5 8',
 charcoalHeart:'M32 18C8-7-8 29 32 55 72 29 56-7 32 18z M31 19l-6 15 12 2-7 15',
 staticRelayCharm:'M31 8 19 29h14l-8 27 25-32H36l9-16z M10 37l8 2m31 3 7-2',
 dewdropChalice:'M14 12h36v13q-1 15-15 18v8h12v6H17v-6h12v-8Q14 40 14 25z M15 18H7v12l12 6m30-18h8v12l-12 6',
 frostguardBrooch:'M32 7 52 18v22L32 57 12 40V18z M32 18v28M21 24l22 14M43 24 21 38M27 20l5 5 5-5m-10 24 5-5 5 5',
 nightfangTooth:'M12 12q32-13 41 7-5 28-28 38l7-26z M17 15l24 10-13 20',
 dawnRelayBell:'M32 8c-7 0-8 10-8 10q-11 4-11 27l-5 5h48l-5-5q0-23-11-27s-1-10-8-10z M24 54q8 9 16 0M20 37l24-10',
 resourceLens:'M28 9a19 19 0 1 1 0 38 19 19 0 1 1 0-38z M42 43l13 13M19 27l8-9 10 9-10 11z',
 trailfinderCompass:'M32 7 52 19v26L32 57 12 45V19z M32 16 43 39 32 33 21 45z M32 16v17',
 heartbloomLocket:'M17 7q15 23 30 0M32 26q-10-18-16-7-11 11 16 35 27-24 16-35-6-11-16 7z M32 34l-8-5 3 13 5 5 5-5 3-13z',
 quakeboundBracers:'M8 19l18-6 6 36-16 6z M38 13l18 6-8 36-16-6z M13 30l13-4m14 0 12 4M15 43l13-4m8 0 12 4',
 furnaceBeads:'M14 17a7 7 0 1 1 0 1zM32 10a7 7 0 1 1 0 1zM50 17a7 7 0 1 1 0 1zM9 36a7 7 0 1 1 0 1zM55 36a7 7 0 1 1 0 1zM20 51a7 7 0 1 1 0 1zM44 51a7 7 0 1 1 0 1zM32 25l8 15-8 11-8-11z',
 tempestSpurs:'M11 12h18v25h14l7 12H13z M44 34l5-10 3 12 10-1-8 8 5 8-13-4',
 tidekeeperPearls:'M12 16Q0 53 32 56 64 53 52 16M10 20a6 6 0 1 1 0 1zM10 37a6 6 0 1 1 0 1zM20 50a6 6 0 1 1 0 1zM44 50a6 6 0 1 1 0 1zM54 37a6 6 0 1 1 0 1zM54 20a6 6 0 1 1 0 1zM32 27l8 11-8 9-8-9z',
 glacierAegis:'M32 5 55 17l-6 30-17 13-17-13-6-30z M32 14 44 36 32 49 20 36z M32 14v35M20 36h24',
 moonfangTalon:'M19 10q39-11 36 20-8 18-33 25l10-20z M31 12q12 17 4 32M14 19l7 23-13 7',
 sunweaverSeal:'M32 8 39 17l11-1-1 12 8 7-10 7-1 12-14-4-13 4-1-12-10-7 8-7-1-12 11 1z M24 25h16v16H24z M18 32h28m-14-14v28',
 supplySatchel:'M12 19h40v33H12z M21 18V9h22v9M12 25l20 13 20-13M27 32h10v11H27z M18 45h5m18 0h5',
 trailblazerMedal:'M13 8h15l4 18 4-18h15L40 33H24z M32 28l14 7v15l-14 9-14-9V35z M32 35l7 12-7-2-7 2z',
 canopyHeart:'M12 10q20 20 40 0M32 27C8 4-3 37 32 57 67 37 56 4 32 27z M17 35h30m-8-7 8 7-8 7m-14-7 7 13',
 mountainheartTotem:'M23 7h18l3 12 11 8-7 29H16L9 27l11-8z M15 29l17 8 17-8M23 18h18M25 43l7 5 7-5',
 infernoSignet:'M18 31a17 17 0 1 0 28 0l-6 8a8 8 0 1 1-16 0z M17 10h30v24H17z M31 13l-6 11 7 7 8-7-7-11',
 thunderheadCrest:'M10 20l7-12 11 9 12-9 14 14-8 16-27-1z M31 22l-9 18h11l-5 16 21-24H37l6-10',
 tideglassPendant:'M15 8q17 16 34 0M21 20h22v7L33 38l10 12v7H21v-7l10-12-10-11z M24 27h16M24 51h16',
 permafrostMantle:'M25 9h14l6 9 12 36-20-6-5 9-5-9-20 6 12-36z M25 9l7 14 7-14M20 27l-5 15 10-4 7 9 7-9 10 4-5-15',
 eclipseTalon:'M14 12q35-14 43 10-4 23-29 35l7-22z M25 14q15 0 17 14-15 8-21-8m-7 6-5 18 13 4',
 dawnbringerDiadem:'M9 23l9 8 6-20 8 15 8-15 6 20 9-8-5 28H14z M15 43h34M32 30l6 7-6 5-6-5z',
 trailwardStandard:'M15 7v51M16 10h36l-8 15 8 16H16z M25 16v18m-6-9h16M23 49l-8-7-8 7',
 prismCompass:'M32 7 56 24l-9 27H17L8 24z M32 15l14 15-14 19-14-19z M18 30h28M32 15v34',
 worldrootHeart:'M32 23C7 1-4 31 32 54 68 31 57 1 32 23z M32 18v34m-15-17 15 8 15-8M32 52l-12 7m12-7 12 7',
 heartOfTheFirstMountain:'M7 45 22 11l10 15L42 7l15 38-8 11H15z M14 42l11-13 8 13 8-12 9 12M26 47l6 6 6-6',
 sunforgeCrown:'M7 18l13 10L24 7l8 17 8-17 4 21 13-10-7 33H14z M14 42h36M32 28l-7 10 7 10 7-10z',
 eyeOfTheTempest:'M5 32q27-38 54 0Q32 70 5 32z M32 19a13 13 0 1 1 0 26 13 13 0 1 1 0-26z M34 21l-9 14h9l-6 12 14-18h-9z',
 oceanmemoryOrb:'M32 8a24 24 0 1 1 0 48 24 24 0 1 1 0-48z M10 34q11-17 22 0t22 0M13 43q10-12 19 0t19 0M22 16h9',
 wintercrownRelic:'M12 18l10 10L32 8l10 20 10-10-5 31H17z M20 39h24M32 23v20m-9-15 18 10m0-10L23 38M22 54h20',
 nightsovereignFangs:'M9 10q22-4 20 20L18 57l-2-25z M36 10q23-4 19 22L43 57l1-25z M14 16l11 9m16-9 10 9',
 beaconOfTheFirstDawn:'M26 24h12v25h9v8H17v-8h9z M20 24 17 13 26 15l6-11 6 11 9-2-3 11z M9 32l9-3m28 0 9 3M12 9l8 5m24 0 8-5',
 wanderkingCrown:'M8 21l12 9L25 9l7 15 7-15 5 21 12-9-7 31H15z M16 43h32M25 34l7-6 7 6-7 6z M13 55h38',
 trailforgersPrism:'M32 8 48 19l8 24-24 15L8 43l8-24z M32 8v50M16 19l16 14 16-14M8 43l24-10 24 10M24 19l8 5 8-5'
};
const colors={leaf:'#83cf87',earth:'#d2ad7a',fire:'#ff9d65',storm:'#c0a0ff',water:'#75c8e7',ice:'#b4ecf1',shadow:'#b8a0d2',light:'#f9dc88',neutral:'#d7c7a5'};
export function equipmentIconSvg(id,{size=64}={}){
 const item=FORGE_GEAR[id],color=colors[item?.element]||colors.neutral,border=GEAR_RARITIES[item?.rarity]?.color||'#b8c7b6';
 const path=outlines[id]||LIVING_EQUIPMENT_OUTLINES[id]||({weapon:'M12 43 43 12l10 10-31 31zM16 38l11 11',armor:'M32 9 52 19v21L32 56 12 40V19z',charm:'M16 9q16 19 32 0M32 25l17 15-17 17-17-17z'}[item?.slot]||'M32 9 55 32 32 55 9 32z');
 const safeSize=Math.max(24,Math.min(256,Number(size)||64));
 return `<svg xmlns="http://www.w3.org/2000/svg" width="${safeSize}" height="${safeSize}" viewBox="0 0 64 64" aria-hidden="true" focusable="false"><rect x="1" y="1" width="62" height="62" rx="13" fill="#192922" stroke="${border}" stroke-opacity=".35"/><path d="${path}" fill="${color}" fill-opacity=".35" stroke="${color}" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"/></svg>`;
}
export const EXPANSION_ICON_IDS=Object.freeze(Object.keys(outlines));
