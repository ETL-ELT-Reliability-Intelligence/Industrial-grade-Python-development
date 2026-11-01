/** HTTP boundary shared by dataset and evaluation queries. */
export class ApiClient {
  constructor(onSuccess) {
    this.onSuccess = onSuccess;
  }

  async get(path, signal) {
    const response = await fetch(`/api/v1${path}`, { signal });
    if (!response.ok) {
      if (response.status === 404) {
        throw new Error(path.includes("/evaluations/") ? "Расчёт не найден для этого dataset." : "Dataset не найден");
      }
      if (response.status === 503) {
        throw new Error("Хранилище недоступно. Проверьте подключение backend к PostgreSQL");
      }
      throw new Error(`Ошибка API (${response.status})`);
    }
    const data = await response.json();
    if (signal?.aborted) throw new DOMException("Запрос отменён", "AbortError");
    this.onSuccess();
    return data;
  }
}
