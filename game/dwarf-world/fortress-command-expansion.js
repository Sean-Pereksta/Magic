/* Dwarf World fortress-command loader.
 * The runtime is fetched as text and injected inside the existing core IIFE so
 * it can extend the real game state without duplicating or replacing the core.
 */
(() => {
  "use strict";
  const MARKER="/*__DW_FORTRESS_COMMAND_EXPANSION__*/";
  const RUNTIME_URL="./dwarf-world/fortress-command-runtime.txt";

  async function prepareCore(coreSource){
    if(typeof coreSource!=="string"||!coreSource.includes("requestAnimationFrame(tick);")){
      throw new Error("Dwarf World core source is not compatible with the fortress expansion loader.");
    }
    if(coreSource.includes(MARKER))return coreSource;
    const response=await fetch(RUNTIME_URL,{cache:"no-store"});
    if(!response.ok)throw new Error(`Could not load Dwarf World fortress expansion (${response.status}).`);
    const runtime=await response.text();
    if(!runtime.includes(MARKER))throw new Error("Dwarf World fortress expansion runtime is incomplete.");
    const needle="\n})();\n</script>";
    if(!coreSource.includes(needle))throw new Error("Could not find the Dwarf World core integration point.");
    return coreSource.replace(needle,`\n${runtime}\n})();\n</script>`);
  }

  window.DwarfWorldFortressExpansion={prepareCore,RUNTIME_URL};
})();
