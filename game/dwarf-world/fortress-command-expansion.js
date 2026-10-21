/* Dwarf World fortress-command loader.
 * Runtime files are fetched as text and injected inside the existing core IIFE
 * so they extend the real game state without duplicating the core simulation.
 */
(() => {
  "use strict";
  const MARKER="/*__DW_FORTRESS_COMMAND_EXPANSION__*/";
  const RUNTIME_URLS=[
    "./dwarf-world/fortress-command-runtime.txt",
    "./dwarf-world/fortress-command-polish-runtime.txt",
    "./dwarf-world/living-simulation.js",
    "./dwarf-world/living-fortress-runtime.txt",
    "./dwarf-world/living-world-runtime.txt",
    "./dwarf-world/living-art-runtime.txt",
    "./dwarf-world/living-interface-runtime.txt"
  ];

  async function prepareCore(coreSource){
    if(typeof coreSource!=="string"||!coreSource.includes("requestAnimationFrame(tick);")){
      throw new Error("Dwarf World core source is not compatible with the fortress expansion loader.");
    }
    coreSource=coreSource.replace(/\r\n?/g,"\n");
    if(coreSource.includes(MARKER))return coreSource;
    // A missing/unconfigured asset host resolves immediately to legacy graphics;
    // configured art has a bounded preload while the shell loading screen is up.
    await window.DwarfWorldArtLoader?.preloadEssentials();
    const chunks=await Promise.all(RUNTIME_URLS.map(async url=>{
      const controller=new AbortController(),timer=setTimeout(()=>controller.abort(),15000);
      try{
        const response=await fetch(url,{cache:"no-store",signal:controller.signal});
        if(!response.ok)throw new Error(`Could not load Dwarf World expansion ${url} (${response.status}).`);
        return (await response.text()).replace(/\r\n?/g,"\n");
      }finally{clearTimeout(timer)}
    }));
    const runtime=chunks.join("\n\n");
    if(!runtime.includes(MARKER))throw new Error("Dwarf World fortress expansion runtime is incomplete.");
    const needle="\n})();\n</script>";
    if(!coreSource.includes(needle))throw new Error("Could not find the Dwarf World core integration point.");
    return coreSource.replace(needle,`\n${runtime}\n})();\n</script>`);
  }

  window.DwarfWorldFortressExpansion={prepareCore,RUNTIME_URLS};
})();
