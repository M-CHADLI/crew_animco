const CONFIG = {
  projectId: 'ddp-bus-web-prd-frlm',
  location: 'global',
  modelId: 'gemini-2.0-flash-001',
  webhookUrl: 'https://chat.googleapis.com/v1/spaces/AAQAcyEWdUQ/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=PxqwQzKYaG3X0jxcnEQji4N5SgbFVRx_sBaXVtvhdmA',

  environment: 'test', // 'test' ou 'prod'
  inputMode: 'bigquery', // 'bigquery' ou 'sheets'

  spreadsheetId: 'COLLE_ICI_L_ID_DU_GOOGLE_SHEET',
  sheetName: 'agent_input',
  logsSheetName: 'agent_logs',

  maxPromptPayloadChars: 50000,
  promptVersion: 'v2.1',

  rayonLimit: 15
};

function runAgentV21() {
  const execution = {
    runId: Utilities.getUuid(),
    startTime: new Date().toISOString(),
    endTime: null,
    status: 'STARTED',
    step: 'init',
    environment: CONFIG.environment,
    inputMode: CONFIG.inputMode,
    promptVersion: CONFIG.promptVersion,
    errorMessage: '',
    vertexFinishReason: '',
    promptChars: 0,
    summary: ''
  };

  try {
    validateConfig();

    execution.step = 'input';
    const agentInput = getAgentInput();
    validateAgentInput(agentInput);
    assertPromptSize(agentInput);

    execution.step = 'prompt';
    const prompt = buildPrompt(agentInput);
    execution.promptChars = prompt.length;

    execution.step = 'vertex';
    const vertexResult = callVertexModel(
      CONFIG.projectId,
      CONFIG.location,
      CONFIG.modelId,
      prompt
    );
    execution.vertexFinishReason = vertexResult.finishReason || '';

    execution.step = 'parse';
    const parsed = parseAgentResponse(vertexResult.reply);
    execution.summary = parsed.summary || '';

    execution.step = 'chat';
    const chatMessage = buildChatMessage(parsed);
    postToGoogleChat(CONFIG.webhookUrl, chatMessage);

    execution.status = 'SUCCESS';
    execution.step = 'done';
    execution.endTime = new Date().toISOString();

    logExecution(execution);
    Logger.log('Workflow terminé avec succès.');
  } catch (error) {
    execution.status = 'FAILED';
    execution.endTime = new Date().toISOString();
    execution.errorMessage = error.message || 'Erreur inconnue';

    logExecution(execution);
    handleError(error, execution);

    throw error;
  }
}

function validateConfig() {
  if (!CONFIG.projectId) throw new Error('CONFIG.projectId manquant.');
  if (!CONFIG.location) throw new Error('CONFIG.location manquant.');
  if (!CONFIG.modelId) throw new Error('CONFIG.modelId manquant.');
  if (!CONFIG.webhookUrl) throw new Error('CONFIG.webhookUrl manquant.');
  if (!['bigquery', 'sheets'].includes(CONFIG.inputMode)) {
    throw new Error('CONFIG.inputMode invalide.');
  }
  if (!['test', 'prod'].includes(CONFIG.environment)) {
    throw new Error('CONFIG.environment invalide.');
  }
  if (!CONFIG.logsSheetName) throw new Error('CONFIG.logsSheetName manquant.');
}

function getAgentInput() {
  if (CONFIG.inputMode === 'bigquery') {
    return getAgentInputFromBigQuery();
  }

  if (CONFIG.inputMode === 'sheets') {
    return getAgentInputFromSheets();
  }

  throw new Error('inputMode invalide : ' + CONFIG.inputMode);
}

function getAgentInputFromBigQuery() {
  const globalQuery = `
WITH params AS (
  SELECT
    CURRENT_DATE("Europe/Paris") AS today,
    DATE_SUB(DATE_TRUNC(CURRENT_DATE("Europe/Paris"), WEEK(MONDAY)), INTERVAL 14 DAY) AS last_2w_start,
    DATE_SUB(DATE_TRUNC(CURRENT_DATE("Europe/Paris"), WEEK(MONDAY)), INTERVAL 1 DAY)  AS last_2w_end,
    DATE_SUB(DATE_SUB(DATE_TRUNC(CURRENT_DATE("Europe/Paris"), WEEK(MONDAY)), INTERVAL 14 DAY), INTERVAL 364 DAY) AS last_2w_start_n1,
    DATE_SUB(DATE_SUB(DATE_TRUNC(CURRENT_DATE("Europe/Paris"), WEEK(MONDAY)), INTERVAL 1 DAY),  INTERVAL 364 DAY) AS last_2w_end_n1
),
fiches_produit AS (
  SELECT
    a.visit_date,
    a.product_number,
    SUM(NULLIF(a.product_detail_view_count, 0)) AS product_detail_view_count
  FROM \`lmfr-ddp-ods-prd.piano_analytics.tf_product_tracking\` a
  CROSS JOIN params p
  WHERE
       a.visit_date BETWEEN p.last_2w_start AND p.last_2w_end
    OR a.visit_date BETWEEN p.last_2w_start_n1 AND p.last_2w_end_n1
  GROUP BY a.visit_date, a.product_number
),
tempo AS (
  SELECT
    productnumber AS product_number,
    DATE(technicalDate) AS date,
    COUNT(customerordernumber) AS customer_order_number,
    SUM(SAFE_CAST(customerOrderLineIncludedTaxAmount AS FLOAT64)) AS Demande
  FROM \`dfdp-ecommerce-lmfr-prod.socle_ecommerce.customerOrderLinesOmniDaily\`
  CROSS JOIN params p
  WHERE
       DATE(technicalDate) BETWEEN p.last_2w_start AND p.last_2w_end
    OR DATE(technicalDate) BETWEEN p.last_2w_start_n1 AND p.last_2w_end_n1
  GROUP BY product_number, date
),
base AS (
  SELECT
    CASE
      WHEN COALESCE(a.visit_date, c.date) BETWEEN p.last_2w_start AND p.last_2w_end THEN '2W_S-1'
      WHEN COALESCE(a.visit_date, c.date) BETWEEN p.last_2w_start_n1 AND p.last_2w_end_n1 THEN '2W_S-1_N-1'
      ELSE 'Hors_perimetre'
    END AS periode,
    SUM(product_detail_view_count) AS fiche_produit_vue,
    SUM(customer_order_number) AS customer_order_number,
    SUM(Demande) AS Demande
  FROM fiches_produit a
  FULL JOIN tempo c
    ON a.visit_date = c.date
   AND a.product_number = c.product_number
  CROSS JOIN params p
  GROUP BY 1
  HAVING SUM(product_detail_view_count) > 0
)
SELECT
  SUM(CASE WHEN periode = '2W_S-1' THEN fiche_produit_vue ELSE 0 END) AS fiche_produit_vue_n,
  SUM(CASE WHEN periode = '2W_S-1_N-1' THEN fiche_produit_vue ELSE 0 END) AS fiche_produit_vue_n1,
  SUM(CASE WHEN periode = '2W_S-1' THEN customer_order_number ELSE 0 END) AS customer_order_number_n,
  SUM(CASE WHEN periode = '2W_S-1_N-1' THEN customer_order_number ELSE 0 END) AS customer_order_number_n1,
  SUM(CASE WHEN periode = '2W_S-1' THEN Demande ELSE 0 END) AS Demande_n,
  SUM(CASE WHEN periode = '2W_S-1_N-1' THEN Demande ELSE 0 END) AS Demande_n1
FROM base
WHERE periode IN ('2W_S-1', '2W_S-1_N-1')
`;

  const rayonQuery = `
WITH params AS (
  SELECT
    CURRENT_DATE("Europe/Paris") AS today,
    DATE_SUB(DATE_TRUNC(CURRENT_DATE("Europe/Paris"), WEEK(MONDAY)), INTERVAL 14 DAY) AS last_2w_start,
    DATE_SUB(DATE_TRUNC(CURRENT_DATE("Europe/Paris"), WEEK(MONDAY)), INTERVAL 1 DAY)  AS last_2w_end,
    DATE_SUB(DATE_SUB(DATE_TRUNC(CURRENT_DATE("Europe/Paris"), WEEK(MONDAY)), INTERVAL 14 DAY), INTERVAL 364 DAY) AS last_2w_start_n1,
    DATE_SUB(DATE_SUB(DATE_TRUNC(CURRENT_DATE("Europe/Paris"), WEEK(MONDAY)), INTERVAL 1 DAY),  INTERVAL 364 DAY) AS last_2w_end_n1
),
fiches_produit AS (
  SELECT
    a.visit_date,
    a.product_number,
    SUM(NULLIF(a.product_detail_view_count, 0)) AS product_detail_view_count
  FROM \`lmfr-ddp-ods-prd.piano_analytics.tf_product_tracking\` a
  CROSS JOIN params p
  WHERE
       a.visit_date BETWEEN p.last_2w_start AND p.last_2w_end
    OR a.visit_date BETWEEN p.last_2w_start_n1 AND p.last_2w_end_n1
  GROUP BY a.visit_date, a.product_number
),
tempo AS (
  SELECT
    productnumber AS product_number,
    DATE(technicalDate) AS date,
    COUNT(customerordernumber) AS customer_order_number,
    SUM(SAFE_CAST(customerOrderLineIncludedTaxAmount AS FLOAT64)) AS Demande
  FROM \`dfdp-ecommerce-lmfr-prod.socle_ecommerce.customerOrderLinesOmniDaily\`
  CROSS JOIN params p
  WHERE
       DATE(technicalDate) BETWEEN p.last_2w_start AND p.last_2w_end
    OR DATE(technicalDate) BETWEEN p.last_2w_start_n1 AND p.last_2w_end_n1
  GROUP BY product_number, date
),
base AS (
  SELECT
    CASE
      WHEN COALESCE(a.visit_date, c.date) BETWEEN p.last_2w_start AND p.last_2w_end THEN '2W_S-1'
      WHEN COALESCE(a.visit_date, c.date) BETWEEN p.last_2w_start_n1 AND p.last_2w_end_n1 THEN '2W_S-1_N-1'
      ELSE 'Hors_perimetre'
    END AS periode,
    IFNULL(concat_department, '99 - Autre') AS rayon,
    SUM(product_detail_view_count) AS fiche_produit_vue,
    SUM(customer_order_number) AS customer_order_number,
    SUM(Demande) AS Demande
  FROM fiches_produit a
  FULL JOIN tempo c
    ON a.visit_date = c.date
   AND a.product_number = c.product_number
  CROSS JOIN params p
  LEFT JOIN \`ddp-bus-web-prd-frlm.Dash_ecommerce_v2.referentiel_article\` b
    ON CAST(COALESCE(c.product_number, a.product_number) AS INT64) = b.product_number
  GROUP BY 1, 2
  HAVING SUM(product_detail_view_count) > 0
),
pivot_rayon AS (
  SELECT
    rayon,
    SUM(CASE WHEN periode = '2W_S-1' THEN fiche_produit_vue ELSE 0 END) AS fiche_produit_vue_n,
    SUM(CASE WHEN periode = '2W_S-1_N-1' THEN fiche_produit_vue ELSE 0 END) AS fiche_produit_vue_n1,
    SUM(CASE WHEN periode = '2W_S-1' THEN customer_order_number ELSE 0 END) AS customer_order_number_n,
    SUM(CASE WHEN periode = '2W_S-1_N-1' THEN customer_order_number ELSE 0 END) AS customer_order_number_n1,
    SUM(CASE WHEN periode = '2W_S-1' THEN Demande ELSE 0 END) AS Demande_n,
    SUM(CASE WHEN periode = '2W_S-1_N-1' THEN Demande ELSE 0 END) AS Demande_n1
  FROM base
  WHERE periode IN ('2W_S-1', '2W_S-1_N-1')
  GROUP BY rayon
)
SELECT
  rayon,
  fiche_produit_vue_n,
  fiche_produit_vue_n1,
  SAFE_DIVIDE(fiche_produit_vue_n - fiche_produit_vue_n1, fiche_produit_vue_n1) AS evo_fiche_produit_vue_pct,
  SAFE_DIVIDE(customer_order_number_n, fiche_produit_vue_n) AS taux_transformation_n,
  SAFE_DIVIDE(customer_order_number_n1, fiche_produit_vue_n1) AS taux_transformation_n1,
  SAFE_DIVIDE(customer_order_number_n, fiche_produit_vue_n)
    - SAFE_DIVIDE(customer_order_number_n1, fiche_produit_vue_n1) AS evo_taux_transformation,
  Demande_n,
  Demande_n1,
  SAFE_DIVIDE(Demande_n - Demande_n1, Demande_n1) AS evo_demande_pct
FROM pivot_rayon
ORDER BY evo_demande_pct ASC
LIMIT ${CONFIG.rayonLimit}
`;

  const globalRows = runBigQuery(CONFIG.projectId, globalQuery);
  const rayonRows = runBigQuery(CONFIG.projectId, rayonQuery);

  return {
    source: 'bigquery',
    analysis_date: new Date().toISOString(),
    scope: '2 dernières semaines complètes vs équivalent N-1',
    global_kpi: globalRows.length > 0 ? mapGlobalRow(globalRows[0]) : {},
    top_rayons: rayonRows.map(mapRayonRow)
  };
}

function getAgentInputFromSheets() {
  const sheet = SpreadsheetApp
    .openById(CONFIG.spreadsheetId)
    .getSheetByName(CONFIG.sheetName);

  if (!sheet) {
    throw new Error('Onglet introuvable : ' + CONFIG.sheetName);
  }

  const values = sheet.getDataRange().getValues();
  if (values.length < 2) {
    throw new Error('Le Google Sheet ne contient pas de données.');
  }

  const headers = values[0].map(h => String(h).trim());
  const rows = values.slice(1).filter(row => row.some(cell => String(cell).trim() !== ''));

  const objects = rows.map(row => {
    const obj = {};
    headers.forEach((header, index) => {
      obj[header] = row[index];
    });
    return obj;
  });

  return {
    source: 'google_sheets',
    analysis_date: new Date().toISOString(),
    scope: 'Données préparées dans Google Sheets',
    rows: objects
  };
}

function validateAgentInput(agentInput) {
  if (!agentInput) {
    throw new Error('agentInput vide.');
  }

  if (agentInput.source === 'bigquery') {
    if (!agentInput.global_kpi || Object.keys(agentInput.global_kpi).length === 0) {
      throw new Error('global_kpi manquant ou vide.');
    }
    if (!agentInput.top_rayons || agentInput.top_rayons.length === 0) {
      throw new Error('top_rayons vide.');
    }
  }

  if (agentInput.source === 'google_sheets') {
    if (!agentInput.rows || agentInput.rows.length === 0) {
      throw new Error('Aucune ligne dans Google Sheets.');
    }
  }
}

function assertPromptSize(agentInput) {
  const payload = JSON.stringify(agentInput);
  if (payload.length > CONFIG.maxPromptPayloadChars) {
    throw new Error('Input trop volumineux pour le modèle : ' + payload.length + ' caractères.');
  }
}

function buildPrompt(agentInput) {
  return `
Tu es un analyste e-commerce.

Version du prompt : ${CONFIG.promptVersion}
Environnement : ${CONFIG.environment}

Analyse les données ci-dessous et réponds STRICTEMENT avec 5 lignes et rien d'autre :

TITLE: <texte très court>
STATUS: <ok|warning|alert>
SUMMARY: <2 phrases maximum>
ANOMALIES: <élément 1> | <élément 2> | <élément 3>
RECOMMENDATION: <phrase actionnable>

Règles :
- aucun markdown
- aucune ligne vide
- aucune phrase avant
- aucune phrase après
- mets en avant les principaux points de variation
- si les données sont insuffisantes, dis-le clairement
- reste concret et orienté pilotage
- utilise les écarts de vues, transformation et demande si disponibles

Données :
${JSON.stringify(agentInput)}
`;
}

function runBigQuery(projectId, query) {
  const queryRequest = {
    query: query,
    useLegacySql: false
  };

  const queryResults = BigQuery.Jobs.query(queryRequest, projectId);
  return queryResults.rows || [];
}

function mapGlobalRow(row) {
  return {
    fiche_produit_vue_n: Number(row.f[0].v || 0),
    fiche_produit_vue_n1: Number(row.f[1].v || 0),
    customer_order_number_n: Number(row.f[2].v || 0),
    customer_order_number_n1: Number(row.f[3].v || 0),
    Demande_n: Number(row.f[4].v || 0),
    Demande_n1: Number(row.f[5].v || 0)
  };
}

function mapRayonRow(row) {
  return {
    rayon: row.f[0].v,
    fiche_produit_vue_n: Number(row.f[1].v || 0),
    fiche_produit_vue_n1: Number(row.f[2].v || 0),
    evo_fiche_produit_vue_pct: Number(row.f[3].v || 0),
    taux_transformation_n: Number(row.f[4].v || 0),
    taux_transformation_n1: Number(row.f[5].v || 0),
    evo_taux_transformation: Number(row.f[6].v || 0),
    Demande_n: Number(row.f[7].v || 0),
    Demande_n1: Number(row.f[8].v || 0),
    evo_demande_pct: Number(row.f[9].v || 0)
  };
}

function callVertexModel(projectId, location, modelId, prompt) {
  const url =
    `https://aiplatform.googleapis.com/v1/projects/${projectId}` +
    `/locations/${location}/publishers/google/models/${modelId}:generateContent`;

  const requestBody = {
    contents: [
      {
        role: 'user',
        parts: [{ text: prompt }]
      }
    ],
    generationConfig: {
      temperature: 0,
      maxOutputTokens: 1000
    }
  };

  const accessToken = ScriptApp.getOAuthToken();

  const response = UrlFetchApp.fetch(url, {
    method: 'post',
    contentType: 'application/json; charset=utf-8',
    headers: {
      Authorization: 'Bearer ' + accessToken
    },
    payload: JSON.stringify(requestBody),
    muteHttpExceptions: true
  });

  const statusCode = response.getResponseCode();
  const responseText = response.getContentText();

  Logger.log('Vertex HTTP ' + statusCode);
  Logger.log(responseText);

  if (statusCode !== 200) {
    throw new Error('Vertex AI call failed. HTTP ' + statusCode + ' - ' + responseText);
  }

  const json = JSON.parse(responseText);
  const reply = json.candidates?.[0]?.content?.parts?.[0]?.text || '';
  const finishReason = json.candidates?.[0]?.finishReason || '';

  Logger.log('Finish reason : ' + finishReason);
  Logger.log('Réponse brute modèle : ' + reply);

  if (finishReason !== 'STOP') {
    throw new Error('Réponse incomplète du modèle. Finish reason : ' + finishReason);
  }

  if (!reply) {
    throw new Error('Réponse vide du modèle.');
  }

  return {
    reply: reply,
    finishReason: finishReason
  };
}

function parseAgentResponse(text) {
  const lines = text
    .split('\n')
    .map(line => line.trim())
    .filter(line => line !== '');

  const result = {
    title: '',
    status: '',
    summary: '',
    anomalies: [],
    recommendation: ''
  };

  lines.forEach(line => {
    if (line.startsWith('TITLE:')) {
      result.title = line.replace('TITLE:', '').trim();
    } else if (line.startsWith('STATUS:')) {
      result.status = line.replace('STATUS:', '').trim().toLowerCase();
    } else if (line.startsWith('SUMMARY:')) {
      result.summary = line.replace('SUMMARY:', '').trim();
    } else if (line.startsWith('ANOMALIES:')) {
      const raw = line.replace('ANOMALIES:', '').trim();
      result.anomalies = raw
        .split('|')
        .map(item => item.trim())
        .filter(item => item !== '');
    } else if (line.startsWith('RECOMMENDATION:')) {
      result.recommendation = line.replace('RECOMMENDATION:', '').trim();
    }
  });

  if (!result.title) throw new Error('TITLE manquant.');
  if (!result.status) throw new Error('STATUS manquant.');
  if (!['ok', 'warning', 'alert'].includes(result.status)) {
    throw new Error('STATUS invalide : ' + result.status);
  }
  if (!result.summary) throw new Error('SUMMARY manquant.');
  if (!result.recommendation) throw new Error('RECOMMENDATION manquant.');

  return result;
}

function buildChatMessage(parsed) {
  return (
    '*' + parsed.title + '*\n\n' +
    '*Statut* : ' + parsed.status + '\n' +
    '*Résumé* : ' + parsed.summary + '\n' +
    '*Anomalies* :\n- ' + parsed.anomalies.join('\n- ') + '\n' +
    '*Recommandation* : ' + parsed.recommendation
  );
}

function postToGoogleChat(webhookUrl, text) {
  const response = UrlFetchApp.fetch(webhookUrl, {
    method: 'post',
    contentType: 'application/json',
    payload: JSON.stringify({ text }),
    muteHttpExceptions: true
  });

  Logger.log('Chat HTTP ' + response.getResponseCode());
  Logger.log(response.getContentText());

  if (response.getResponseCode() >= 300) {
    throw new Error('Erreur Google Chat : HTTP ' + response.getResponseCode());
  }
}

function handleError(error, execution) {
  const fallbackMessage =
    '*Agent e-commerce - erreur*\n\n' +
    '*Étape* : ' + (execution.step || 'inconnue') + '\n' +
    '*Message* : ' + (error.message || 'Erreur inconnue') + '\n' +
    '*Run ID* : ' + execution.runId + '\n' +
    '*Heure* : ' + new Date().toISOString();

  try {
    postToGoogleChat(CONFIG.webhookUrl, fallbackMessage);
  } catch (chatError) {
    Logger.log('Impossible de notifier l\'erreur dans Google Chat : ' + chatError.message);
  }
}

function logExecution(execution) {
  try {
    const ss = SpreadsheetApp.openById(CONFIG.spreadsheetId);
    let sheet = ss.getSheetByName(CONFIG.logsSheetName);

    if (!sheet) {
      sheet = ss.insertSheet(CONFIG.logsSheetName);
      sheet.appendRow([
        'run_id',
        'start_time',
        'end_time',
        'status',
        'step',
        'environment',
        'input_mode',
        'prompt_version',
        'vertex_finish_reason',
        'prompt_chars',
        'summary',
        'error_message'
      ]);
    }

    sheet.appendRow([
      execution.runId || '',
      execution.startTime || '',
      execution.endTime || '',
      execution.status || '',
      execution.step || '',
      execution.environment || '',
      execution.inputMode || '',
      execution.promptVersion || '',
      execution.vertexFinishReason || '',
      execution.promptChars || '',
      execution.summary || '',
      execution.errorMessage || ''
    ]);
  } catch (logError) {
    Logger.log('Erreur logExecution : ' + logError.message);
  }
}

function deleteAllTriggers() {
  const triggers = ScriptApp.getProjectTriggers();
  triggers.forEach(trigger => ScriptApp.deleteTrigger(trigger));
}

function createDailyTrigger() {
  ScriptApp.newTrigger('runAgentV21')
    .timeBased()
    .everyDays(1)
    .atHour(8)
    .create();
}

function createWeeklyTrigger() {
  ScriptApp.newTrigger('runAgentV21')
    .timeBased()
    .onWeekDay(ScriptApp.WeekDay.MONDAY)
    .atHour(8)
    .create();
}

function testBigQueryMode() {
  const previousMode = CONFIG.inputMode;
  CONFIG.inputMode = 'bigquery';
  runAgentV21();
  CONFIG.inputMode = previousMode;
}

function testSheetsMode() {
  const previousMode = CONFIG.inputMode;
  CONFIG.inputMode = 'sheets';
  runAgentV21();
  CONFIG.inputMode = previousMode;
}
