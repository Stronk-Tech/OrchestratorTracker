const Database = require('better-sqlite3');
const path = require('path');
const fs = require('fs');

class Storage {
  constructor(dbPath = './data/storage.db') {
    // Ensure directory exists
    const dir = path.dirname(dbPath);
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }

    this.db = new Database(dbPath);
    this.db.pragma('journal_mode = WAL'); // Write-Ahead Logging for better concurrency
    this.db.pragma('synchronous = NORMAL'); // Balance between safety and performance

    this.init();
  }

  init() {
    // Create table if it doesn't exist
    const createTable = this.db.prepare(`
      CREATE TABLE IF NOT EXISTS storage (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        updated_at INTEGER NOT NULL
      )
    `);
    createTable.run();

    // Prepare statements for better performance
    this.getStmt = this.db.prepare('SELECT value FROM storage WHERE key = ?');
    this.setStmt = this.db.prepare(`
      INSERT OR REPLACE INTO storage (key, value, updated_at) 
      VALUES (?, ?, ?)
    `);
    this.deleteStmt = this.db.prepare('DELETE FROM storage WHERE key = ?');
    this.getAllStmt = this.db.prepare('SELECT key, value FROM storage');
  }

  async getItem(key) {
    try {
      const row = this.getStmt.get(key);
      if (!row) return null;

      return JSON.parse(row.value);
    } catch (error) {
      console.error(`Error getting item ${key}:`, error.message);
      return null;
    }
  }

  async setItem(key, value) {
    try {
      const jsonValue = JSON.stringify(value);
      const timestamp = Date.now();

      this.setStmt.run(key, jsonValue, timestamp);
      console.log(`Stored data for key: ${key}`);
      return true;
    } catch (error) {
      console.error(`Error setting item ${key}:`, error.message);
      return false;
    }
  }

  async removeItem(key) {
    try {
      this.deleteStmt.run(key);
      return true;
    } catch (error) {
      console.error(`Error removing item ${key}:`, error.message);
      return false;
    }
  }

  async clear() {
    try {
      this.db.prepare('DELETE FROM storage').run();
      return true;
    } catch (error) {
      console.error('Error clearing storage:', error.message);
      return false;
    }
  }

  async getAllKeys() {
    try {
      const rows = this.getAllStmt.all();
      return rows.map(row => row.key);
    } catch (error) {
      console.error('Error getting all keys:', error.message);
      return [];
    }
  }

  // Batch operations for better performance
  async setItems(items) {
    const transaction = this.db.transaction((items) => {
      const timestamp = Date.now();
      for (const [key, value] of items) {
        const jsonValue = JSON.stringify(value);
        this.setStmt.run(key, jsonValue, timestamp);
      }
    });

    try {
      transaction(items);
      console.log(`Batch stored ${items.length} items`);
      return true;
    } catch (error) {
      console.error('Error in batch set:', error.message);
      return false;
    }
  }

  // Get storage statistics
  getStats() {
    try {
      const countStmt = this.db.prepare('SELECT COUNT(*) as count FROM storage');
      const sizeStmt = this.db.prepare('SELECT SUM(LENGTH(value)) as size FROM storage');

      const count = countStmt.get().count;
      const size = sizeStmt.get().size || 0;

      return { count, size };
    } catch (error) {
      console.error('Error getting storage stats:', error.message);
      return { count: 0, size: 0 };
    }
  }

  close() {
    if (this.db) {
      this.db.close();
    }
  }
}

module.exports = Storage; 