// 任务历史：IndexedDB 持久化。
const TaskHistory = (() => {
  const DB_NAME = 'worker-pool-db';
  const STORE = 'tasks';
  let dbPromise = null;

  function open() {
    if (dbPromise) return dbPromise;
    dbPromise = new Promise((resolve, reject) => {
      const req = indexedDB.open(DB_NAME, 1);
      req.onupgradeneeded = () => {
        const db = req.result;
        if (!db.objectStoreNames.contains(STORE)) {
          db.createObjectStore(STORE, { keyPath: 'id' });
        }
      };
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error);
    });
    return dbPromise;
  }

  async function save(task) {
    try {
      const db = await open();
      const record = {
        id: task.id,
        name: task.name,
        priority: task.priority,
        status: task.status,
        attempts: task.attempts,
        workerLabel: task.workerLabel,
        submittedAt: task.submittedAt,
        startedAt: task.startedAt,
        endedAt: task.endedAt,
        duration: task.duration,
        result: task.result,
        error: task.error,
      };
      await new Promise((resolve, reject) => {
        const tx = db.transaction(STORE, 'readwrite');
        tx.objectStore(STORE).put(record);
        tx.oncomplete = resolve;
        tx.onerror = () => reject(tx.error);
      });
    } catch (err) {
      console.warn('历史写入失败', err);
    }
  }

  async function loadAll(limit = 200) {
    try {
      const db = await open();
      return await new Promise((resolve, reject) => {
        const tx = db.transaction(STORE, 'readonly');
        const req = tx.objectStore(STORE).getAll();
        req.onsuccess = () => {
          const rows = req.result.sort((a, b) => b.endedAt - a.endedAt);
          resolve(rows.slice(0, limit));
        };
        req.onerror = () => reject(req.error);
      });
    } catch (err) {
      console.warn('历史读取失败', err);
      return [];
    }
  }

  async function clear() {
    try {
      const db = await open();
      await new Promise((resolve, reject) => {
        const tx = db.transaction(STORE, 'readwrite');
        tx.objectStore(STORE).clear();
        tx.oncomplete = resolve;
        tx.onerror = () => reject(tx.error);
      });
    } catch (err) {
      console.warn('历史清空失败', err);
    }
  }

  return { save, loadAll, clear };
})();
