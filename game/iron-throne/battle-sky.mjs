// Full-screen triangle at the far plane. Draw after opaque geometry so the cloud
// shader runs only on the visible sky, including at steep tactical camera angles.
export function createBattleSky(gl, climate, compile) {
  const vertex = compile(gl, gl.VERTEX_SHADER, `attribute vec2 aScreen;
    varying vec2 uv;
    void main(){uv=aScreen; gl_Position=vec4(aScreen,1.,1.);}`);
  const fragment = compile(gl, gl.FRAGMENT_SHADER, `precision highp float;
    varying vec2 uv;
    uniform vec3 uForward,uRight,uUp,uSun,uHorizon;
    uniform float uTime,uPhase;uniform sampler2D uNoise;
    float noise(vec2 p) {return texture2D(uNoise,(p+.5)/128.).r;}
    void main(){
      vec3 ray=normalize(uForward+uv.x*uRight+uv.y*uUp);
      float high=clamp(ray.y,0.,1.), sun=max(0.,dot(ray,uSun));
      vec3 sky=mix(uHorizon,vec3(.15,.33,.57),sqrt(high));
      sky+=vec3(.24,.16,.07)*pow(sun,18.)+vec3(.8,.68,.4)*smoothstep(.9988,.9998,sun);
      vec2 plane=ray.xz/max(.075,ray.y)*1.3 + uPhase;
      vec2 front=plane+vec2(uTime*.011,uTime*.004);
      vec2 back=plane*.63+vec2(-uTime*.006,uTime*.002+13.);
      float lowCloud=smoothstep(.53,.76,.64*noise(front)+.36*noise(front*2.35));
      float highCloud=smoothstep(.57,.88,noise(back));
      float cloud=(lowCloud*.72+highCloud*.18)*smoothstep(.005,.13,high);
      sky=mix(sky,mix(vec3(.67,.74,.79),vec3(1.,.9,.74),sun*.7+lowCloud*.25),cloud);
      gl_FragColor=vec4(sky,1.);
    }`);
  const program = gl.createProgram();
  gl.attachShader(program,vertex); gl.attachShader(program,fragment); gl.linkProgram(program);
  gl.deleteShader(vertex); gl.deleteShader(fragment);
  if (!gl.getProgramParameter(program,gl.LINK_STATUS)) throw new Error(gl.getProgramInfoLog(program));
  const buffer=gl.createBuffer(), position=gl.getAttribLocation(program,'aScreen');
  const locations=Object.fromEntries(['Forward','Right','Up','Sun','Horizon','Time','Phase'].map(key=>[key,gl.getUniformLocation(program,'u'+key)]));
  const dusk=Math.abs(climate.hour-12)/6;
  const horizon=[.70+.10*dusk,.80-.06*dusk,.84-.14*dusk];
  gl.bindBuffer(gl.ARRAY_BUFFER,buffer);
  gl.bufferData(gl.ARRAY_BUFFER,new Float32Array([-1,-1,3,-1,-1,3]),gl.STATIC_DRAW);
  return {horizon,draw(forward,right,up,time,ext){
    gl.useProgram(program); gl.bindBuffer(gl.ARRAY_BUFFER,buffer);
    ext?.vertexAttribDivisorANGLE(position,0); gl.enableVertexAttribArray(position); gl.vertexAttribPointer(position,2,gl.FLOAT,false,0,0);
    gl.uniform3fv(locations.Forward,forward); gl.uniform3fv(locations.Right,right); gl.uniform3fv(locations.Up,up);
    gl.uniform3fv(locations.Sun,climate.sun); gl.uniform3fv(locations.Horizon,horizon);
    gl.uniform1f(locations.Phase,climate.phase); gl.uniform1f(locations.Time,time);
    gl.depthFunc(gl.LEQUAL); gl.depthMask(false); gl.drawArrays(gl.TRIANGLES,0,3);
    gl.depthMask(true); gl.depthFunc(gl.LESS);
  },dispose(){gl.deleteBuffer(buffer);gl.deleteProgram(program);}};
}
