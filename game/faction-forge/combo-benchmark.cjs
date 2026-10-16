'use strict';
// Compatibility entry point for the expanded five-unit balance matrix.
require('./balance-benchmark.cjs').main().catch(error=>{console.error(error);process.exitCode=1;});
