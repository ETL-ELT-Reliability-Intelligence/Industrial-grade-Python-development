/** Selection and request lifecycle; rendering and transport are injected. */
export class EvaluationController {
  constructor(api, view, onSelect) {
    this.api = api;
    this.view = view;
    this.onSelect = onSelect;
    this.selection = {};
    this.offset = 0;
    this.pending = null;
  }

  show(selection) {
    if (selection.dataset !== this.selection.dataset || selection.scope !== this.selection.scope) {
      this.offset = 0;
    }
    this.selection = { ...selection };
    return this.reload();
  }

  choose(evaluationId) {
    this.selection.evaluation = evaluationId;
    this.onSelect(evaluationId, false);
    return this.reload();
  }

  changePage(offset) {
    this.offset = offset;
    return this.reload();
  }

  /** Replace pending requests and render only responses for the current selection. */
  async reload() {
    this.pending?.abort();
    const request = new AbortController();
    this.pending = request;
    const { dataset, scope, evaluation } = this.selection;
    if (!dataset || !scope) {
      this.view.clear();
      return;
    }
    this.view.loading();
    const path = `/datasets/${encodeURIComponent(dataset)}`;
    const query = new URLSearchParams({ scope_id: scope, limit: 50, offset: this.offset });
    let historyLoaded = false;
    try {
      const page = await this.api.get(`${path}/evaluations?${query}`, request.signal);
      if (request !== this.pending) return;
      historyLoaded = true;
      let selected = evaluation;
      if (selected === null && page.items.length) {
        selected = page.items[0].evaluation_id;
        this.selection.evaluation = selected;
        this.onSelect(selected, true);
      }
      this.view.history(page, selected, id => this.choose(id), offset => this.changePage(offset));
      if (selected !== null) {
        if (!selected.trim()) throw new Error("В ссылке указан пустой идентификатор расчёта.");
        const record = await this.api.get(`${path}/evaluations/${encodeURIComponent(selected)}`, request.signal);
        if (request !== this.pending) return;
        if (record.dataset_id !== dataset || record.scope_id !== scope) {
          throw new Error("Расчёт не относится к выбранной области проверки.");
        }
        this.view.detail(record);
      } else {
        // Only a successful empty history permits the legacy evidence view.
        const quality = await this.api.get(`${path}/quality?${new URLSearchParams({ scope_id: scope })}`, request.signal);
        if (request !== this.pending) return;
        this.view.legacy(quality);
      }
    } catch (error) {
      if (request.signal.aborted || request !== this.pending) return;
      this.view.error(error instanceof TypeError ? "Не удалось связаться с API." : error.message, () => this.reload(), historyLoaded);
    } finally {
      if (request === this.pending) this.view.idle();
    }
  }
}
