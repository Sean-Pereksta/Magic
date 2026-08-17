from pathlib import Path
import re

PATH = Path("game/pdfreader.html")
text = PATH.read_text(encoding="utf-8")
original = text


def replace_once(old: str, new: str, label: str) -> None:
    global text
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{label}: expected exactly one match, found {count}")
    text = text.replace(old, new, 1)


def regex_once(pattern: str, replacement: str, label: str) -> None:
    global text
    text, count = re.subn(pattern, replacement, text, count=1, flags=re.S)
    if count != 1:
        raise SystemExit(f"{label}: expected exactly one regex match, found {count}")


replace_once(
'''    #speechHint {
      flex: 1 0 210px;
      min-width: 210px;
      color: var(--muted);
      font-size: 12px;
      line-height: 1.25;
    }
''',
'''    #speechHint {
      flex: 1 0 210px;
      min-width: 210px;
      color: var(--muted);
      font-size: 12px;
      line-height: 1.25;
    }

    .speech-progress {
      flex: 0 0 250px;
      min-width: 210px;
      max-width: 320px;
      padding: 6px 8px;
      border: 1px solid var(--line);
      border-radius: 9px;
      background: var(--panel);
      font: 11px/1.2 system-ui, sans-serif;
    }

    .speech-progress.hidden {
      display: none;
    }

    .speech-progress-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
      margin-bottom: 5px;
      color: var(--muted);
    }

    #speechProgressLabel {
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }

    #speechProgressPercent {
      flex: 0 0 auto;
      color: var(--ink);
      font-weight: 700;
    }

    .speech-progress-track {
      overflow: hidden;
      height: 6px;
      border-radius: 999px;
      background: var(--line);
    }

    #speechProgressFill {
      width: 0;
      height: 100%;
      border-radius: inherit;
      background: var(--accent);
      transition: width 160ms ease;
    }

    .speech-progress.indeterminate #speechProgressFill {
      width: 34% !important;
      animation: speech-progress-slide 1.05s ease-in-out infinite;
    }

    @keyframes speech-progress-slide {
      0% { transform: translateX(-110%); }
      50% { transform: translateX(100%); }
      100% { transform: translateX(300%); }
    }
''',
"speech progress styles",
)

replace_once(
'''        <span id="speechHint">Natural AI loads on first use, then stays cached by the browser.</span>
      </div>
''',
'''        <span id="speechHint">Natural AI loads on first use, then stays cached by the browser.</span>
        <div id="speechProgress" class="speech-progress hidden" role="status" aria-live="polite" aria-atomic="true">
          <div class="speech-progress-head">
            <span id="speechProgressLabel">Preparing natural voice…</span>
            <span id="speechProgressPercent"></span>
          </div>
          <div class="speech-progress-track" aria-hidden="true">
            <div id="speechProgressFill"></div>
          </div>
        </div>
      </div>
''',
"speech progress markup",
)

replace_once(
'''      speechRateLabel: document.querySelector("#speechRateLabel"),
      speechHint: document.querySelector("#speechHint"),
      emptyState: document.querySelector("#emptyState"),
''',
'''      speechRateLabel: document.querySelector("#speechRateLabel"),
      speechHint: document.querySelector("#speechHint"),
      speechProgress: document.querySelector("#speechProgress"),
      speechProgressLabel: document.querySelector("#speechProgressLabel"),
      speechProgressPercent: document.querySelector("#speechProgressPercent"),
      speechProgressFill: document.querySelector("#speechProgressFill"),
      emptyState: document.querySelector("#emptyState"),
''',
"speech progress element refs",
)

replace_once(
'''    const naturalSpeechSupported = typeof Audio === "function" && typeof WebAssembly === "object";
''',
'''    const naturalSpeechSupported = Boolean((window.AudioContext || window.webkitAudioContext) && typeof WebAssembly === "object");
''',
"natural support detection",
)

replace_once(
'''        currentAudio: null,
        currentAudioUrl: null,
        naturalLoading: false,
''',
'''        naturalAudioContext: null,
        naturalPlayback: null,
        naturalLoading: false,
''',
"natural audio state",
)

replace_once(
'''    function setStatus(message) {
      elements.statusMessage.textContent = message;
    }
''',
'''    function setStatus(message) {
      elements.statusMessage.textContent = message;
    }

    function setNaturalProgress(label = "", percent = null) {
      const visible = Boolean(label);
      elements.speechProgress.classList.toggle("hidden", !visible);
      if (!visible) {
        elements.speechProgress.classList.remove("indeterminate");
        elements.speechProgressFill.style.width = "0%";
        elements.speechProgressPercent.textContent = "";
        return;
      }

      elements.speechProgressLabel.textContent = label;
      const numeric = Number(percent);
      if (!Number.isFinite(numeric)) {
        elements.speechProgress.classList.add("indeterminate");
        elements.speechProgressFill.style.width = "34%";
        elements.speechProgressPercent.textContent = "";
        return;
      }

      const clamped = Math.max(0, Math.min(100, numeric));
      elements.speechProgress.classList.remove("indeterminate");
      elements.speechProgressFill.style.width = `${clamped.toFixed(1)}%`;
      elements.speechProgressPercent.textContent = `${Math.round(clamped)}%`;
    }
''',
"natural progress helper",
)

regex_once(
    r'''    function releaseNaturalAudio\(\) \{.*?\n    \}\n\n    async function detectNaturalDevice''',
'''    function naturalAudioContextCtor() {
      return window.AudioContext || window.webkitAudioContext || null;
    }

    async function ensureNaturalAudioContext() {
      const AudioContextCtor = naturalAudioContextCtor();
      if (!AudioContextCtor) {
        throw new Error("Web Audio is unavailable in this browser.");
      }

      if (!state.speech.naturalAudioContext || state.speech.naturalAudioContext.state === "closed") {
        state.speech.naturalAudioContext = new AudioContextCtor({ latencyHint: "playback" });
      }

      if (state.speech.naturalAudioContext.state === "suspended") {
        await state.speech.naturalAudioContext.resume();
      }
      return state.speech.naturalAudioContext;
    }

    function naturalPlaybackTime(playback = state.speech.naturalPlayback) {
      if (!playback) return 0;
      if (playback.paused) return playback.offset;
      return Math.max(0, Math.min(playback.duration, playback.context.currentTime - playback.startedAt));
    }

    function stopNaturalSource(playback) {
      if (!playback?.source) return;
      playback.source.onended = null;
      try {
        playback.source.stop();
      } catch {
        // The source may already have ended.
      }
      try {
        playback.source.disconnect();
      } catch {
        // Ignore disconnect failures during cleanup.
      }
      playback.source = null;
    }

    function startNaturalPlaybackSource(playback) {
      stopNaturalSource(playback);
      if (playback.offset >= playback.duration - 0.005) {
        playback.resolve?.("ended");
        return;
      }

      const source = playback.context.createBufferSource();
      source.buffer = playback.buffer;
      source.connect(playback.context.destination);
      playback.source = source;
      playback.paused = false;
      playback.startedAt = playback.context.currentTime - playback.offset;
      source.onended = () => {
        if (state.speech.naturalPlayback !== playback || playback.paused) return;
        playback.offset = playback.duration;
        playback.resolve?.("ended");
      };
      source.start(0, playback.offset);
    }

    function releaseNaturalAudio() {
      const playback = state.speech.naturalPlayback;
      if (!playback) return;
      if (playback.timer) clearInterval(playback.timer);
      stopNaturalSource(playback);
      state.speech.naturalPlayback = null;
      const resolve = playback.resolve;
      playback.resolve = null;
      resolve?.("stopped");
    }

    function pauseNaturalAudio() {
      const playback = state.speech.naturalPlayback;
      if (!playback || playback.paused) return;
      playback.offset = naturalPlaybackTime(playback);
      playback.paused = true;
      stopNaturalSource(playback);
    }

    async function resumeNaturalAudio() {
      const playback = state.speech.naturalPlayback;
      if (!playback || !playback.paused) return;
      if (playback.context.state === "suspended") await playback.context.resume();
      startNaturalPlaybackSource(playback);
    }

    function naturalSamplesFrom(rawAudio) {
      const source = rawAudio?.data ?? rawAudio?.audio;
      let samples;
      if (source instanceof Float32Array) {
        samples = source;
      } else if (ArrayBuffer.isView(source)) {
        samples = Float32Array.from(source);
      } else if (Array.isArray(source)) {
        samples = Float32Array.from(source);
      } else {
        throw new Error("The natural voice engine returned no PCM audio samples.");
      }

      if (samples.length < 800) {
        throw new Error("The natural voice engine returned an empty or truncated audio buffer.");
      }

      let peak = 0;
      const stride = Math.max(1, Math.floor(samples.length / 12000));
      for (let index = 0; index < samples.length; index += stride) {
        const sample = samples[index];
        if (!Number.isFinite(sample)) {
          throw new Error("The natural voice engine returned non-finite audio samples.");
        }
        peak = Math.max(peak, Math.abs(sample));
      }
      if (peak < 0.00001) {
        throw new Error("The natural voice engine returned silent audio.");
      }
      if (peak > 20) {
        throw new Error("The natural voice engine returned an invalid audio amplitude.");
      }
      return samples;
    }

    async function playNaturalBuffer(rawAudio, chunk, model, chunkStart, session) {
      const context = await ensureNaturalAudioContext();
      if (!state.speech.active || session !== state.speech.session) return "stopped";

      const samples = naturalSamplesFrom(rawAudio);
      const sampleRate = Number(rawAudio?.sampling_rate || rawAudio?.sampleRate || 24000);
      if (!Number.isFinite(sampleRate) || sampleRate < 8000 || sampleRate > 96000) {
        throw new Error(`Natural voice returned an invalid sample rate: ${sampleRate}`);
      }

      setNaturalProgress("Preparing decoded audio buffer…", 92);
      const buffer = context.createBuffer(1, samples.length, sampleRate);
      buffer.copyToChannel(samples, 0);
      if (!Number.isFinite(buffer.duration) || buffer.duration <= 0.03 || buffer.duration > 180) {
        throw new Error(`Natural voice returned an invalid audio duration: ${buffer.duration}`);
      }

      releaseNaturalAudio();
      const playback = {
        context,
        buffer,
        source: null,
        offset: 0,
        startedAt: 0,
        duration: buffer.duration,
        paused: false,
        timer: null,
        resolve: null,
      };
      state.speech.naturalPlayback = playback;

      setNaturalProgress("Audio fully buffered · starting…", 100);
      highlightSpeechWord(chunkStart, true);
      elements.speechHint.textContent = `Natural AI reading page ${state.speech.page} of ${state.pageCount}…`;

      const outcome = await new Promise(resolve => {
        playback.resolve = resolve;
        startNaturalPlaybackSource(playback);
        setNaturalProgress();

        playback.timer = window.setInterval(() => {
          if (state.speech.naturalPlayback !== playback || playback.paused) return;
          if (!state.speech.active || session !== state.speech.session) return;
          const ratio = playback.duration > 0 ? naturalPlaybackTime(playback) / playback.duration : 0;
          const estimatedChar = Math.floor(Math.max(0, Math.min(1, ratio)) * Math.max(0, chunk.text.length - 1));
          const localOffset = wordOffsetForCharacter(chunk.charStarts, estimatedChar);
          const globalIndex = Math.min(chunk.endIndex - 1, chunkStart + localOffset);
          if (globalIndex !== state.speech.wordIndex) {
            state.speech.wordIndex = globalIndex;
            state.speech.cursorPage = state.speech.page;
            state.speech.cursorWord = globalIndex;
            highlightSpeechWord(globalIndex, true);
            if (globalIndex % 12 === 0) queuePositionSave();
          }
        }, 120);
      });

      if (playback.timer) clearInterval(playback.timer);
      return outcome;
    }

    async function detectNaturalDevice''',
    "replace HTML audio playback with predecoded Web Audio",
)

regex_once(
    r'''    async function ensureNaturalTts\(\) \{.*?\n    \}\n\n    function stopSpeech''',
'''    async function ensureNaturalTts() {
      if (kokoroTts) return kokoroTts;
      if (kokoroLoadPromise) return kokoroLoadPromise;
      if (!state.speech.naturalSupported) {
        throw new Error("Natural text to speech is unavailable in this browser.");
      }

      state.speech.naturalLoading = true;
      elements.speechHint.textContent = "Loading the natural voice model for the first time…";
      setNaturalProgress("Starting natural voice engine…", 3);
      updateControlState();

      kokoroLoadPromise = (async () => {
        try {
          const { KokoroTTS } = await import(CONFIG.kokoroModule);
          setNaturalProgress("Choosing the best local AI engine…", 7);
          const device = await detectNaturalDevice();
          const dtype = device === "webgpu" ? "fp32" : "q8";
          let highestModelProgress = 7;
          const progress_callback = info => {
            if (!state.speech.naturalLoading || !state.speech.active) return;
            const raw = Number(info?.progress);
            if (Number.isFinite(raw)) {
              const normalized = raw <= 1 ? raw * 100 : raw;
              highestModelProgress = Math.max(highestModelProgress, Math.min(84, 8 + normalized * 0.76));
              setNaturalProgress("Loading natural voice model…", highestModelProgress);
            } else {
              setNaturalProgress("Loading natural voice model…");
            }
          };
          const instance = await KokoroTTS.from_pretrained(CONFIG.kokoroModel, {
            device,
            dtype,
            progress_callback,
          });
          kokoroTts = instance;
          state.speech.naturalReady = true;
          state.speech.naturalDevice = device;
          setNaturalProgress("Natural voice model ready", 100);
          showToast(`Natural AI voice ready${device === "webgpu" ? " with GPU acceleration" : ""}.`);
          return instance;
        } catch (error) {
          kokoroLoadPromise = null;
          setNaturalProgress();
          console.error("Natural narration failed to load:", error);
          throw error;
        } finally {
          state.speech.naturalLoading = false;
          if (!state.speech.active) setNaturalProgress();
          updateSpeechHint();
          updateControlState();
        }
      })();

      return kokoroLoadPromise;
    }

    function stopSpeech''',
    "model loading progress",
)

replace_once(
'''      releaseNaturalAudio();

      state.speech.active = false;
''',
'''      releaseNaturalAudio();
      setNaturalProgress();

      state.speech.active = false;
''',
"hide progress on stop",
)

replace_once(
'''      if (!state.pdfDocument) return;

      stopSpeech({ silent: true });
''',
'''      if (!state.pdfDocument) return;

      if (state.speech.engine === "natural") {
        try {
          // Prime Web Audio while this call still originates from the user's click/tap.
          // This avoids losing browser audio permission during a long first model load.
          await ensureNaturalAudioContext();
        } catch (error) {
          console.error("Could not initialize Web Audio for natural narration:", error);
          if (state.speech.systemSupported) {
            state.speech.engine = "system";
            elements.speechEngineSelect.value = "system";
            setStoredValue(STORAGE_KEYS.speechEngine, "system");
            populateSpeechVoices();
            showToast("Natural audio could not initialize, so narration switched to System Voice.");
          } else {
            showToast("Natural audio could not initialize in this browser.");
            return;
          }
        }
      }

      stopSpeech({ silent: true });
''',
"prime Web Audio on user gesture",
)

regex_once(
    r'''    async function speakNaturalChunk\(model, session\) \{.*?\n    \}\n\n    function toggleSpeech''',
'''    async function speakNaturalChunk(model, session) {
      const chunkStart = state.speech.wordIndex;
      const chunk = buildNaturalSpeechChunk(model.words, chunkStart);
      if (!chunk.text) {
        state.speech.wordIndex = model.words.length;
        return;
      }

      let tts;
      try {
        tts = await ensureNaturalTts();
      } catch (error) {
        if (!state.speech.active || session !== state.speech.session) return;
        if (state.speech.systemSupported) {
          state.speech.engine = "system";
          elements.speechEngineSelect.value = "system";
          setStoredValue(STORAGE_KEYS.speechEngine, "system");
          populateSpeechVoices();
          setNaturalProgress();
          showToast("Natural AI could not load, so narration switched to System Voice.");
          speakSystemChunk(model, session);
          return;
        }
        state.speech.active = false;
        state.speech.paused = false;
        setNaturalProgress();
        elements.speechHint.textContent = "Natural narration could not load in this browser.";
        updateControlState();
        showToast("Natural narration could not load.");
        return;
      }

      if (!state.speech.active || session !== state.speech.session || state.speech.engine !== "natural") return;

      setNaturalProgress(`Generating natural narration · page ${state.speech.page}…`);
      elements.speechHint.textContent = `Generating natural narration for page ${state.speech.page}…`;
      let rawAudio;
      try {
        rawAudio = await tts.generate(chunk.text, {
          voice: state.speech.naturalVoice,
          speed: state.speech.rate,
        });
      } catch (error) {
        console.error("Natural narration generation failed:", error);
        if (!state.speech.active || session !== state.speech.session) return;
        setNaturalProgress();
        if (state.speech.systemSupported) {
          state.speech.engine = "system";
          elements.speechEngineSelect.value = "system";
          setStoredValue(STORAGE_KEYS.speechEngine, "system");
          populateSpeechVoices();
          showToast("Natural AI generation failed, so narration switched to System Voice.");
          speakSystemChunk(model, session);
          return;
        }
        state.speech.active = false;
        state.speech.paused = false;
        elements.speechHint.textContent = "Natural narration stopped because audio generation failed.";
        updateControlState();
        showToast("Natural narration could not generate this passage.");
        return;
      }

      if (!state.speech.active || session !== state.speech.session || state.speech.engine !== "natural") return;

      try {
        const outcome = await playNaturalBuffer(rawAudio, chunk, model, chunkStart, session);
        if (outcome === "stopped" || !state.speech.active || session !== state.speech.session) return;
        state.speech.wordIndex = chunk.endIndex;
        state.speech.cursorPage = state.speech.page;
        state.speech.cursorWord = Math.max(0, Math.min(model.words.length - 1, chunk.endIndex));
        queuePositionSave();
      } catch (error) {
        console.error("Natural narration audio validation/playback failed:", error);
        releaseNaturalAudio();
        setNaturalProgress();
        if (!state.speech.active || session !== state.speech.session) return;
        if (state.speech.systemSupported) {
          state.speech.engine = "system";
          elements.speechEngineSelect.value = "system";
          setStoredValue(STORAGE_KEYS.speechEngine, "system");
          populateSpeechVoices();
          showToast("Natural audio was invalid, so narration switched to System Voice.");
          speakSystemChunk(model, session);
          return;
        }
        state.speech.active = false;
        state.speech.paused = false;
        elements.speechHint.textContent = "Natural narration audio could not be prepared safely.";
        updateControlState();
        showToast("Natural narration audio playback failed.");
        return;
      } finally {
        releaseNaturalAudio();
        setNaturalProgress();
      }
    }

    function toggleSpeech''',
    "safe natural chunk generation/playback",
)

replace_once(
'''          if (state.speech.engine === "natural") {
            state.speech.currentAudio?.play().catch(() => {});
          } else {
            window.speechSynthesis.resume();
          }
''',
'''          if (state.speech.engine === "natural") {
            resumeNaturalAudio().catch(error => {
              console.error("Could not resume natural narration:", error);
              showToast("Natural narration could not resume.");
            });
          } else {
            window.speechSynthesis.resume();
          }
''',
"natural resume",
)

replace_once(
'''          if (state.speech.engine === "natural") {
            state.speech.currentAudio?.pause();
          } else {
            window.speechSynthesis.pause();
          }
''',
'''          if (state.speech.engine === "natural") {
            pauseNaturalAudio();
          } else {
            window.speechSynthesis.pause();
          }
''',
"natural pause",
)

# Guard against the old Blob/HTMLAudio path surviving anywhere in the natural implementation.
for forbidden in ("state.speech.currentAudio", "state.speech.currentAudioUrl", "new Audio(url)", "rawAudio.toBlob()"):
    if forbidden in text:
        raise SystemExit(f"old natural audio path still present: {forbidden}")

required = [
    'id="speechProgress"',
    "progress_callback",
    "context.createBuffer(1, samples.length, sampleRate)",
    "Audio fully buffered · starting…",
    "pauseNaturalAudio()",
    "resumeNaturalAudio()",
]
for marker in required:
    if marker not in text:
        raise SystemExit(f"required marker missing after patch: {marker}")

if text == original:
    raise SystemExit("patch made no changes")

PATH.write_text(text, encoding="utf-8")
print(f"Patched {PATH}: {len(original)} -> {len(text)} characters")
