export class InputController {
  constructor(root = document) {
    this.root = root;
    this.held = new Set();
    this.pressed = new Set();
    this.axis = 0;
    this.mobileJump = false;
    this.enabled = true;
    this.onAnyInput = null;
    this.bindKeyboard();
  }

  bindKeyboard() {
    window.addEventListener("keydown", (event) => {
      const key = event.key.toLowerCase();
      if (["arrowleft", "arrowright", "arrowup", "arrowdown", " "].includes(key)) event.preventDefault();
      if (!event.repeat) this.pressed.add(key);
      this.held.add(key);
      if (this.onAnyInput) this.onAnyInput();
    });
    window.addEventListener("keyup", (event) => this.held.delete(event.key.toLowerCase()));
    window.addEventListener("blur", () => {
      this.held.clear();
      this.axis = 0;
    });
  }

  bindMobile(joystick, stick, jumpButton, lightButton, rainButton) {
    if (joystick && stick) {
      const update = (event) => {
        const rect = joystick.getBoundingClientRect();
        const dx = event.clientX - (rect.left + rect.width / 2);
        const dy = event.clientY - (rect.top + rect.height / 2);
        const radius = rect.width * 0.34;
        const length = Math.hypot(dx, dy) || 1;
        const scale = Math.min(radius, length) / length;
        const x = dx * scale;
        const y = dy * scale;
        stick.style.transform = `translate(${x}px, ${y}px)`;
        this.axis = Math.abs(x / radius) < 0.12 ? 0 : x / radius;
        if (y > radius * 0.42) this.held.add("arrowdown");
        else this.held.delete("arrowdown");
      };
      const reset = () => {
        this.axis = 0;
        this.held.delete("arrowdown");
        stick.style.transform = "translate(0, 0)";
      };
      joystick.addEventListener("pointerdown", (event) => {
        joystick.setPointerCapture(event.pointerId);
        update(event);
        if (this.onAnyInput) this.onAnyInput();
      });
      joystick.addEventListener("pointermove", (event) => {
        if (event.buttons || event.pressure > 0) update(event);
      });
      joystick.addEventListener("pointerup", reset);
      joystick.addEventListener("pointercancel", reset);
    }
    this.bindTouchButton(jumpButton, "jump");
    this.bindTouchButton(lightButton, "q");
    this.bindTouchButton(rainButton, "e");
  }

  bindTouchButton(button, key) {
    if (!button) return;
    button.addEventListener("pointerdown", (event) => {
      event.preventDefault();
      this.held.add(key);
      this.pressed.add(key);
      if (this.onAnyInput) this.onAnyInput();
    });
    const release = () => this.held.delete(key);
    button.addEventListener("pointerup", release);
    button.addEventListener("pointercancel", release);
  }

  consume(key) {
    const normalized = key.toLowerCase();
    if (!this.pressed.has(normalized)) return false;
    this.pressed.delete(normalized);
    return true;
  }

  consumeAny(keys) {
    return keys.some((key) => this.consume(key));
  }

  horizontal() {
    if (!this.enabled) return 0;
    if (Math.abs(this.axis) > 0.05) return this.axis;
    return Number(this.held.has("d") || this.held.has("arrowright")) -
      Number(this.held.has("a") || this.held.has("arrowleft"));
  }

  jumpPressed() {
    return this.enabled && this.consumeAny(["jump", "w", "arrowup", " "]);
  }

  jumpHeld() {
    return this.enabled && ["jump", "w", "arrowup", " "].some((key) => this.held.has(key));
  }

  downHeld() {
    return this.enabled && (this.held.has("s") || this.held.has("arrowdown"));
  }

  clearFrame() {
    this.pressed.clear();
  }
}

