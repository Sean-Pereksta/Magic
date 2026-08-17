from pathlib import Path

path = Path("game/pdfreader.html")
text = path.read_text(encoding="utf-8")
original = text


def replace_once(old: str, new: str, label: str) -> None:
    global text
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{label}: expected exactly one match, found {count}")
    text = text.replace(old, new, 1)


def replace_between(start_marker: str, end_marker: str, replacement: str, label: str) -> None:
    global text
    start = text.find(start_marker)
    if start < 0:
        raise SystemExit(f"{label}: start marker not found")
    end = text.find(end_marker, start)
    if end < 0:
        raise SystemExit(f"{label}: end marker not found")
    text = text[:start] + replacement + text[end:]


replace_once(
    "      renderTask: null,\n      loadSerial: 0,",
    "      renderTask: null,\n      renderSerial: 0,\n      loadSerial: 0,",
    "render serial state",
)

old_open_cancel = '''      if (state.renderTask) {
        try {
          state.renderTask.cancel();
        } catch {
          // No active render to cancel.
        }
        state.renderTask = null;
      }
'''
new_open_cancel = '''      if (state.renderTask) {
        const previousRender = state.renderTask;
        try {
          previousRender.cancel();
          await previousRender.promise;
        } catch (error) {
          if (error?.name !== "RenderingCancelledException") {
            console.warn("Previous PDF render did not shut down cleanly:", error);
          }
        } finally {
          if (state.renderTask === previousRender) state.renderTask = null;
        }
      }
'''
replace_once(old_open_cancel, new_open_cancel, "openPdf render cancellation")

new_render = '''    async function renderCurrentPage() {
      if (!state.pdfDocument) return;

      const loadSerial = state.loadSerial;
      const renderSerial = ++state.renderSerial;
      const requestedPage = state.pageNumber;
      const pdfDocument = state.pdfDocument;

      state.currentPageTextModel = null;
      elements.textLayer.replaceChildren();
      setStatus(`Loading page ${requestedPage} of ${state.pageCount}…`);
      updateControlState();

      const page = await pdfDocument.getPage(requestedPage);
      if (
        loadSerial !== state.loadSerial ||
        renderSerial !== state.renderSerial ||
        pdfDocument !== state.pdfDocument ||
        requestedPage !== state.pageNumber
      ) return;

      if (state.renderTask) {
        const previousRender = state.renderTask;
        try {
          previousRender.cancel();
          await previousRender.promise;
        } catch (error) {
          if (error?.name !== "RenderingCancelledException") {
            console.warn("Previous PDF page render did not cancel cleanly:", error);
          }
        } finally {
          if (state.renderTask === previousRender) state.renderTask = null;
        }

        if (
          loadSerial !== state.loadSerial ||
          renderSerial !== state.renderSerial ||
          pdfDocument !== state.pdfDocument ||
          requestedPage !== state.pageNumber
        ) return;
      }

      const viewport = page.getViewport({ scale: state.scale });
      const outputScale = Math.min(window.devicePixelRatio || 1, 2.25);
      const canvas = elements.canvas;
      const context = canvas.getContext("2d", { alpha: false });

      canvas.width = Math.max(1, Math.floor(viewport.width * outputScale));
      canvas.height = Math.max(1, Math.floor(viewport.height * outputScale));
      canvas.style.width = `${Math.floor(viewport.width)}px`;
      canvas.style.height = `${Math.floor(viewport.height)}px`;

      elements.canvasWrap.style.width = `${Math.floor(viewport.width)}px`;
      elements.canvasWrap.style.height = `${Math.floor(viewport.height)}px`;
      elements.textLayer.style.width = `${Math.floor(viewport.width)}px`;
      elements.textLayer.style.height = `${Math.floor(viewport.height)}px`;
      setViewState("document");

      const transform = outputScale !== 1
        ? [outputScale, 0, 0, outputScale, 0, 0]
        : null;

      const renderTask = page.render({
        canvasContext: context,
        viewport,
        transform,
        background: "rgb(255,255,255)"
      });
      state.renderTask = renderTask;

      try {
        await renderTask.promise;
      } catch (error) {
        if (error?.name !== "RenderingCancelledException") throw error;
        return;
      } finally {
        if (state.renderTask === renderTask) state.renderTask = null;
      }

      if (
        loadSerial !== state.loadSerial ||
        renderSerial !== state.renderSerial ||
        pdfDocument !== state.pdfDocument ||
        requestedPage !== state.pageNumber
      ) return;

      await renderInteractiveTextLayer(page, viewport, loadSerial, requestedPage, renderSerial);
      if (
        loadSerial !== state.loadSerial ||
        renderSerial !== state.renderSerial ||
        requestedPage !== state.pageNumber
      ) return;

      elements.viewerScroll.scrollTo({ top: 0, left: 0, behavior: "auto" });
      elements.currentTitle.textContent = state.activeEntry?.title || "PDF";
      setStatus(`Page ${requestedPage} of ${state.pageCount}`);
      updateControlState();
      queuePositionSave();
    }

'''
replace_between(
    "    async function renderCurrentPage() {",
    "    async function getPageTextModel(",
    new_render,
    "renderCurrentPage",
)

# Keep the text layer tied to the exact page that produced it. This prevents a
# fast page change from caching or wiring the previous page's text as the new page.
segment_start = text.find("    async function renderInteractiveTextLayer(")
segment_end = text.find("    function updateSpeechHint(", segment_start)
if segment_start < 0 or segment_end < 0:
    raise SystemExit("renderInteractiveTextLayer segment not found")
segment = text[segment_start:segment_end]
segment = segment.replace(
    "async function renderInteractiveTextLayer(page, viewport, serial)",
    "async function renderInteractiveTextLayer(page, viewport, serial, pageNumber, renderSerial)",
)
segment = segment.replace(
    "const model = await getPageTextModel(state.pageNumber, page);\n      if (serial !== state.loadSerial) return;",
    "const model = await getPageTextModel(pageNumber, page);\n      if (serial !== state.loadSerial || renderSerial !== state.renderSerial || pageNumber !== state.pageNumber) return;",
)
segment = segment.replace('wordSpan.dataset.pageNumber = String(state.pageNumber);', 'wordSpan.dataset.pageNumber = String(pageNumber);')
segment = segment.replace('startSpeechAt(state.pageNumber, wordIndex);', 'startSpeechAt(pageNumber, wordIndex);')
if segment.count("pageNumber") < 6:
    raise SystemExit("renderInteractiveTextLayer page pinning did not apply")
text = text[:segment_start] + segment + text[segment_end:]

# Smaller chunks reduce WASM latency and make bad PDF text less likely to poison
# a long generated passage.
replace_once(
    "while (endIndex < words.length && pieces.length < 100)",
    "while (endIndex < words.length && pieces.length < 60)",
    "natural chunk word limit",
)
replace_once(
    "if (pieces.length && length + extra > 520) break;",
    "if (pieces.length && length + extra > 320) break;",
    "natural chunk character limit",
)
replace_once(
    '''        text: pieces.join(" ")
          .replace(/\\s+([,.;:!?])/g, "$1")
          .replace(/([“‘(])\\s+/g, "$1"),''',
    '''        text: pieces.join(" ")
          .normalize("NFKC")
          .replace(/[\\u0000-\\u001F\\u007F-\\u009F]/g, " ")
          .replace(/\\u00AD/g, "")
          .replace(/\\s+([,.;:!?])/g, "$1")
          .replace(/([“‘(])\\s+/g, "$1")
          .replace(/\\s{2,}/g, " ")
          .trim(),''',
    "natural text cleanup",
)

new_samples = '''    function naturalSamplesFrom(rawAudio) {
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
      let sumSquares = 0;
      for (let index = 0; index < samples.length; index += 1) {
        const sample = samples[index];
        if (!Number.isFinite(sample)) {
          throw new Error("The natural voice engine returned non-finite audio samples.");
        }
        const absolute = Math.abs(sample);
        peak = Math.max(peak, absolute);
        sumSquares += sample * sample;
      }

      const rms = Math.sqrt(sumSquares / samples.length);
      if (peak < 0.00001 || rms < 0.000001) {
        throw new Error("The natural voice engine returned silent audio.");
      }

      // Some WASM/ONNX combinations return valid waveform-shaped PCM with a
      // scale much hotter than Web Audio's expected -1..1 range. Normalize it
      // rather than throwing away the whole passage and breaking narration.
      if (peak > 1.05) {
        if (peak > 1e12) {
          throw new Error("The natural voice engine returned numerically corrupted audio.");
        }
        const gain = 0.98 / peak;
        const normalized = new Float32Array(samples.length);
        for (let index = 0; index < samples.length; index += 1) {
          normalized[index] = Math.max(-1, Math.min(1, samples[index] * gain));
        }
        console.warn("Natural narration returned hot PCM; normalized before playback.", {
          peak,
          rms,
          gain,
          samples: samples.length,
        });
        samples = normalized;
      }

      return samples;
    }

'''
replace_between(
    "    function naturalSamplesFrom(rawAudio) {",
    "    async function playNaturalBuffer(",
    new_samples,
    "naturalSamplesFrom",
)

# The user's failing path is ort-wasm. Prefer correctness over download/startup
# time there; FP32 is larger but avoids the q8 path that produced gibberish/hot PCM.
replace_once(
    '          const dtype = device === "webgpu" ? "fp32" : "q8";',
    '          const dtype = "fp32";',
    "natural model dtype",
)

new_continue = '''    async function continueSpeech(session) {
      while (state.speech.active && session === state.speech.session && state.speech.page <= state.pageCount) {
        const speechPage = state.speech.page;
        const pageAlreadyReady =
          state.pageNumber === speechPage &&
          state.currentPageTextModel?.pageNumber === speechPage;

        if (!pageAlreadyReady) {
          elements.speechHint.textContent = `Loading page ${speechPage} of ${state.pageCount} for narration…`;
          if (state.speech.engine === "natural") {
            setNaturalProgress(`Loading page ${speechPage} of ${state.pageCount}…`);
          }
          await goToPage(speechPage, { fromSpeech: true, forceRender: true });
          if (!state.speech.active || session !== state.speech.session) return;
        }

        const model = state.currentPageTextModel?.pageNumber === speechPage
          ? state.currentPageTextModel
          : await getPageTextModel(speechPage);
        if (!state.speech.active || session !== state.speech.session) return;

        if (!model.words.length || state.speech.wordIndex >= model.words.length) {
          if (speechPage >= state.pageCount) {
            state.speech.page = state.pageCount + 1;
            break;
          }

          const nextPage = speechPage + 1;
          state.speech.page = nextPage;
          state.speech.wordIndex = 0;
          state.speech.cursorPage = nextPage;
          state.speech.cursorWord = 0;
          queuePositionSave();

          elements.speechHint.textContent = `Finished page ${speechPage}. Loading page ${nextPage} of ${state.pageCount}…`;
          if (state.speech.engine === "natural") {
            setNaturalProgress(`Loading next page · ${nextPage} of ${state.pageCount}…`);
          }

          await goToPage(nextPage, { fromSpeech: true, forceRender: true });
          if (!state.speech.active || session !== state.speech.session) return;

          // Do not generate or speak the next chunk until the visible page and
          // selectable text layer are definitely ready.
          if (state.currentPageTextModel?.pageNumber !== nextPage) {
            await getPageTextModel(nextPage);
          }
          if (!state.speech.active || session !== state.speech.session) return;
          continue;
        }

        if (state.speech.engine === "natural") {
          await speakNaturalChunk(model, session);
          if (!state.speech.active || session !== state.speech.session) return;
          continue;
        }

        speakSystemChunk(model, session);
        return;
      }

      if (state.speech.active && session === state.speech.session) {
        state.speech.active = false;
        state.speech.paused = false;
        state.speech.currentUtterance = null;
        releaseNaturalAudio();
        setNaturalProgress();
        elements.speechHint.textContent = "Finished reading the document.";
        updateControlState();
        showToast("Finished reading the document.");
      }
    }

'''
replace_between(
    "    async function continueSpeech(session) {",
    "    function speakSystemChunk(",
    new_continue,
    "continueSpeech",
)

new_go_to = '''    async function goToPage(pageNumber, { fromSpeech = false, forceRender = false } = {}) {
      if (!state.pdfDocument) return;
      if (!fromSpeech && state.speech.active) stopSpeech({ silent: true });
      const next = Math.max(1, Math.min(state.pageCount, Math.floor(Number(pageNumber) || 1)));
      const pageIsReady = state.currentPageTextModel?.pageNumber === next;
      if (next === state.pageNumber && !forceRender && pageIsReady) {
        updateControlState();
        return;
      }

      state.pageNumber = next;
      setStatus(`Loading page ${next} of ${state.pageCount}…`);

      if (state.zoomMode === "fit") {
        await setFitWidthScale();
      } else {
        await renderCurrentPage();
      }
    }

'''
replace_between(
    "    async function goToPage(pageNumber, { fromSpeech = false } = {}) {",
    "    async function changeZoom(",
    new_go_to,
    "goToPage",
)

if text == original:
    raise SystemExit("Patch made no changes")

required = [
    "renderSerial: 0",
    "normalized before playback",
    'const dtype = "fp32";',
    "Loading next page",
    "forceRender = false",
    "pageNumber, renderSerial",
]
for marker in required:
    if marker not in text:
        raise SystemExit(f"Missing expected marker after patch: {marker}")

path.write_text(text, encoding="utf-8")
print("Applied PDF reader narration/page-loading fix")
