'use strict';

// IndexedDB 任务历史存储
const TaskHistoryDB = (() => {
  const DB_NAME = 'worker-pool-demo';
  const STORE = 'task-history';
  let dbPromise = null;

  function open() {
    if (dbPromise) return dbPromise;
    dbPromise = new Promise((resolve, reject) => {
      const req = indexedDB.open(DB_NAME, 1);
      req.onupgradeneeded = () => {
        const db = req.result;
        if (!db.objectStoreNames.contains(STORE)) {
          const store = db.createObjectStore(STORE, { keyPath: 'taskId' });
          store.createIndex('finishedAt', 'finishedAt');
        }
      };
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error);
    });
    return dbPromise;
  }

  async function put(record) {
    try {
      const db = await open();
      return await new Promise((resolve, reject) => {
        const tx = db.transaction(STORE, 'readwrite');
        tx.objectStore(STORE).put(record);
        tx.oncomplete = resolve;
        tx.onerror = () => reject(tx.error);
      });
    } catch (err) {
      console.warn('[db] 写入历史失败（降级为仅内存展示）:', err);
    }
  }

  async function getAll(limit = 200) {
    try {
      const db = await open();
      return await new Promise((resolve, reject) => {
        const req = db.transaction(STORE, 'readonly').objectStore(STORE).getAll();
        req.onsuccess = () => {
          const rows = req.result || [];
          rows.sort((a, b) => (b.finishedAt || 0) - (a.finishedAt || 0));
          resolve(rows.slice(0, limit));
        };
        req.onerror = () => reject(req.error);
      });
    } catch (err) {
      console.warn('[db] 读取历史失败:', err);
      return [];
    }
  }

  async function clear() {
    try {
      const db = await open();
      return await new Promise((resolve, reject) => {
        const tx = db.transaction(STORE, 'readwrite');
        tx.objectStore(STORE).clear();
        tx.oncomplete = resolve;
        tx.onerror = () => reject(tx.error);
      });
    } catch (err) {
      console.warn('[db] 清空历史失败:', err);
    }
  }

  return { put, getAll, clear };
})();
