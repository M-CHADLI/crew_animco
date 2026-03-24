// ============================================================
// TEMPLATE BIGQUERY APPS SCRIPT — LECTURE SEULE
// ============================================================
// Comment utiliser ce template :
// 1. Copier ce fichier dans votre projet Apps Script
// 2. Configurer TEMPLATE_CONFIG ci-dessous
// 3. Ajouter vos requêtes dans MY_QUERIES
// 4. Lancer testMyQueries() pour tester
// ============================================================
// Pré-requis IAM (demander à l'admin GCP) :
//   - roles/bigquery.dataViewer  (lecture données)
//   - roles/bigquery.jobUser     (exécuter des requêtes)
//   NE PAS donner : dataEditor, dataOwner, admin
// ============================================================

const TEMPLATE_CONFIG = {
  projectId: 'ddp-bus-web-prd-frlm',
  maxBytesBilled: '5000000000',  // 5 Go max par requête (protection coûts)
  maxRows: 10000,                // limite de lignes retournées
  timeoutMs: 30000               // timeout requête (30s)
};

// ============================================================
// VOS REQUÊTES — Ajoutez les ici
// ============================================================
// Chaque membre de l'équipe ajoute ses requêtes dans cet objet.
// Clé = nom lisible, Valeur = requête SQL (SELECT uniquement)

const MY_QUERIES = {

  // Exemple 1 : requête simple
  exemple_simple: `
    SELECT current_date() AS today, 'Hello BigQuery' AS message
  `,

  // Exemple 2 : lire une table avec filtre
  exemple_table: `
    SELECT *
    FROM \`ddp-bus-web-prd-frlm.Dash_ecommerce_v2.referentiel_article\`
    LIMIT 100
  `,

  // Ajoutez vos requêtes ici :
  // ma_requete: `SELECT ... FROM ... WHERE ...`

};

// ============================================================
// MOTEUR BIGQUERY SECURISE — NE PAS MODIFIER
// ============================================================

/**
 * Exécute une requête BigQuery en lecture seule.
 * Bloque toute requête qui n'est pas un SELECT.
 *
 * @param {string} sql - La requête SQL
 * @param {Object} [options] - Options optionnelles
 * @param {string} [options.projectId] - Override du projectId
 * @param {string} [options.maxBytesBilled] - Override limite bytes
 * @returns {Object} { headers: string[], rows: any[][], rowCount: number, bytesProcessed: string }
 */
function bqQuery(sql, options) {
  options = options || {};
  var projectId = options.projectId || TEMPLATE_CONFIG.projectId;
  var maxBytes = options.maxBytesBilled || TEMPLATE_CONFIG.maxBytesBilled;

  // --- SECURITE : bloquer tout sauf SELECT ---
  _assertReadOnly(sql);

  var queryRequest = {
    query: sql,
    useLegacySql: false,
    maxResults: TEMPLATE_CONFIG.maxRows,
    timeoutMs: TEMPLATE_CONFIG.timeoutMs,
    maximumBytesBilled: maxBytes
  };

  var response = BigQuery.Jobs.query(queryRequest, projectId);

  var headers = (response.schema && response.schema.fields)
    ? response.schema.fields.map(function(f) { return f.name; })
    : [];

  var rows = (response.rows || []).map(function(row) {
    return row.f.map(function(cell) { return cell.v; });
  });

  return {
    headers: headers,
    rows: rows,
    rowCount: rows.length,
    bytesProcessed: response.totalBytesProcessed || '0'
  };
}

/**
 * Exécute une requête et écrit le résultat dans un onglet Google Sheets.
 *
 * @param {string} sql - La requête SQL
 * @param {string} sheetName - Nom de l'onglet destination
 * @param {Object} [options] - Options pour bqQuery
 */
function bqQueryToSheet(sql, sheetName, options) {
  var result = bqQuery(sql, options);
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName(sheetName);

  if (!sheet) {
    sheet = ss.insertSheet(sheetName);
  } else {
    sheet.clearContents();
  }

  if (result.headers.length > 0) {
    sheet.getRange(1, 1, 1, result.headers.length).setValues([result.headers]);
    sheet.getRange(1, 1, 1, result.headers.length).setFontWeight('bold');
  }

  if (result.rows.length > 0) {
    sheet.getRange(2, 1, result.rows.length, result.rows[0].length).setValues(result.rows);
  }

  Logger.log(
    sheetName + ' : ' + result.rowCount + ' lignes écrites (' +
    _formatBytes(result.bytesProcessed) + ' scannés)'
  );

  return result;
}

// ============================================================
// SECURITE
// ============================================================

function _assertReadOnly(sql) {
  var cleaned = sql.replace(/--.*$/gm, '').replace(/\/\*[\s\S]*?\*\//g, '');
  var forbidden = /^\s*(INSERT|UPDATE|DELETE|DROP|TRUNCATE|CREATE|ALTER|MERGE|CALL|EXECUTE)/i;

  var statements = cleaned.split(';').filter(function(s) { return s.trim().length > 0; });

  statements.forEach(function(stmt) {
    if (forbidden.test(stmt.trim())) {
      throw new Error(
        'BLOQUE : seules les requêtes SELECT sont autorisées.\n' +
        'Requête détectée : ' + stmt.trim().substring(0, 80) + '...'
      );
    }
  });
}

function _formatBytes(bytes) {
  var b = Number(bytes);
  if (b < 1024) return b + ' B';
  if (b < 1048576) return (b / 1024).toFixed(1) + ' KB';
  if (b < 1073741824) return (b / 1048576).toFixed(1) + ' MB';
  return (b / 1073741824).toFixed(2) + ' GB';
}

// ============================================================
// FONCTIONS DE TEST — Lancez celles-ci
// ============================================================

/**
 * Test rapide : exécute toutes les requêtes de MY_QUERIES
 * et affiche les résultats dans les logs.
 */
function testMyQueries() {
  var queryNames = Object.keys(MY_QUERIES);

  queryNames.forEach(function(name) {
    Logger.log('--- Exécution : ' + name + ' ---');
    try {
      var result = bqQuery(MY_QUERIES[name]);
      Logger.log('Colonnes : ' + result.headers.join(', '));
      Logger.log('Lignes   : ' + result.rowCount);
      Logger.log('Scannés  : ' + _formatBytes(result.bytesProcessed));
      if (result.rows.length > 0) {
        Logger.log('Ligne 1  : ' + JSON.stringify(result.rows[0]));
      }
      Logger.log('OK');
    } catch (e) {
      Logger.log('ERREUR : ' + e.message);
    }
    Logger.log('');
  });
}

/**
 * Exporte toutes les requêtes vers des onglets Google Sheets.
 * Chaque requête crée un onglet avec le nom de la clé.
 */
function exportAllToSheets() {
  var queryNames = Object.keys(MY_QUERIES);

  queryNames.forEach(function(name) {
    Logger.log('Export : ' + name);
    try {
      bqQueryToSheet(MY_QUERIES[name], 'bq_' + name);
      Logger.log('OK');
    } catch (e) {
      Logger.log('ERREUR sur ' + name + ' : ' + e.message);
    }
  });

  SpreadsheetApp.getActiveSpreadsheet().toast(
    queryNames.length + ' requêtes exportées', 'BigQuery Export', 5
  );
}

/**
 * Test de sécurité : vérifie que les requêtes dangereuses sont bloquées.
 */
function testSecurityBlock() {
  var dangerous = [
    "DELETE FROM `dataset.table` WHERE 1=1",
    "DROP TABLE `dataset.table`",
    "INSERT INTO `dataset.table` VALUES (1)",
    "UPDATE `dataset.table` SET col = 1",
    "TRUNCATE TABLE `dataset.table`"
  ];

  dangerous.forEach(function(sql) {
    try {
      bqQuery(sql);
      Logger.log('FAIL — aurait dû être bloqué : ' + sql);
    } catch (e) {
      Logger.log('OK — bloqué : ' + sql.substring(0, 40));
    }
  });
}
