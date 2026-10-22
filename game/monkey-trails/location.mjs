// Permission setup verifies access only. Coordinates never enter a save or the walking filter.
export function locationProblem(){
  if(!globalThis.isSecureContext)return 'insecure';
  if(!globalThis.navigator?.geolocation)return 'unsupported';
  try{const policy=globalThis.document?.permissionsPolicy||globalThis.document?.featurePolicy;if(policy?.allowsFeature&&!policy.allowsFeature('geolocation'))return 'policy';}catch{}
  return null;
}

export class LocationAccess {
  constructor(onChange=()=>{}){this.onChange=onChange;this.status='unknown';this.pending=false;this.generation=0;this.permission=null;this.timer=null;}
  update(status){this.status=status;this.onChange(this);}
  async refresh(){
    if(this.pending)return;
    const problem=locationProblem();if(problem){this.update(problem);return;}
    const generation=this.generation;
    try{
      if(!navigator.permissions?.query){if(this.status!=='ready')this.update('unknown');return;}
      const permission=await navigator.permissions.query({name:'geolocation'});
      if(generation!==this.generation||this.pending)return;
      if(this.permission!==permission){if(this.permission)this.permission.onchange=null;this.permission=permission;permission.onchange=()=>{if(!this.pending)this.update(permission.state==='granted'&&this.status==='ready'?'ready':permission.state);};}
      // Preserve useful request errors while the underlying permission is unchanged.
      if(permission.state==='denied')this.update('denied');
      else if(!['ready','timeout','unavailable'].includes(this.status)||permission.state==='prompt'&&this.status==='ready')this.update(permission.state);
    }catch{if(generation!==this.generation||this.pending)return;if(!['ready','timeout','unavailable'].includes(this.status))this.update('unknown');}
  }
  request(){
    if(this.pending)return;
    const problem=locationProblem();if(problem){this.update(problem);return;}
    if(globalThis.document?.hidden){this.update('hidden');return;}
    const generation=++this.generation;this.pending=true;this.update('requesting');
    const finish=status=>{if(generation!==this.generation)return;clearTimeout(this.timer);this.timer=null;this.pending=false;this.generation++;this.update(status);};
    // Some browser/phone combinations never return while a permission dialog is blocked.
    this.timer=setTimeout(()=>finish('timeout'),20000);
    try{
      // Deliberately before any await: retain the user's button-tap activation.
      navigator.geolocation.getCurrentPosition(()=>finish('ready'),error=>finish(error.code===1?'denied':error.code===3?'timeout':'unavailable'),{enableHighAccuracy:true,maximumAge:0,timeout:12000});
    }catch{finish('unavailable');}
  }
  cancel(){if(!this.pending)return;this.generation++;clearTimeout(this.timer);this.timer=null;this.pending=false;this.update('prompt');}
  reportError(error){this.cancel();this.update(locationProblem()||(error.code===1?'denied':error.code===3?'timeout':'unavailable'));}
}
