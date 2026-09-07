/* Dwarf World fortress-command loader.
 * Runtime files are fetched as text and injected inside the existing core IIFE
 * so they extend the real game state without duplicating the core simulation.
 */
(() => {
  "use strict";
  const MARKER="/*__DW_FORTRESS_COMMAND_EXPANSION__*/";
  const RUNTIME_URLS=[
    "./dwarf-world/fortress-command-runtime.txt",
    "./dwarf-world/fortress-command-polish-runtime.txt"
  ];

  async function prepareCore(coreSource){
    if(typeof coreSource!=="string"||!coreSource.includes("requestAnimationFrame(tick);")){
      throw new Error("Dwarf World core source is not compatible with the fortress expansion loader.");
    }
    if(coreSource.includes(MARKER))return coreSource;
    const chunks=[];
    for(const url of RUNTIME_URLS){
      const response=await fetch(url,{cache:"no-store"});
      if(!response.ok)throw new Error(`Could not load Dwarf World fortress expansion (${response.status}).`);
      chunks.push(await response.text());
    }
    const runtime=chunks.join("\n\n");
    if(!runtime.includes(MARKER))throw new Error("Dwarf World fortress expansion runtime is incomplete.");
    const needle="\n})();\n</script>";
    if(!coreSource.includes(needle))throw new Error("Could not find the Dwarf World core integration point.");
    return coreSource.replace(needle,`\n${runtime}\n})();\n</script>`);
  }

  window.DwarfWorldFortressExpansion={prepareCore,RUNTIME_URLS};
})();
