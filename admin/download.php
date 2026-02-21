<?php
session_start();
if (!isset($_SESSION['logged_in'])) {
    http_response_code(403);
    echo 'Forbidden';
    exit;
}

$id = (int)($_GET['id'] ?? 0);
if ($id <= 0) {
    http_response_code(400);
    echo 'Bad file id';
    exit;
}

try {
    $host = getenv('MYSQL_HOST') ?: 'mysql';
    $port = getenv('MYSQL_PORT') ?: '3306';
    $db = getenv('MYSQL_DB') ?: 'chel3d_db';
    $user = getenv('MYSQL_USER') ?: 'chel3d_user';
    $pass = getenv('MYSQL_PASSWORD') ?: '';
    $token = getenv('BOT_TOKEN') ?: '';

    $pdo = new PDO("mysql:host={$host};port={$port};dbname={$db};charset=utf8mb4", $user, $pass, [
        PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
    ]);

    $stmt = $pdo->prepare('SELECT * FROM order_files WHERE id=:id');
    $stmt->execute([':id' => $id]);
    $file = $stmt->fetch();
    if (!$file) {
        http_response_code(404);
        echo 'File record not found';
        exit;
    }

    if (!$token) {
        http_response_code(500);
        echo 'BOT_TOKEN is not configured';
        exit;
    }

    $getFileUrl = 'https://api.telegram.org/bot' . $token . '/getFile?file_id=' . urlencode($file['telegram_file_id']);
    $metaJson = @file_get_contents($getFileUrl);
    if ($metaJson === false) {
        http_response_code(502);
        echo 'Cannot reach Telegram API';
        exit;
    }

    $meta = json_decode($metaJson, true);
    if (!isset($meta['ok']) || !$meta['ok'] || !isset($meta['result']['file_path'])) {
        http_response_code(502);
        echo 'Telegram did not return file path';
        exit;
    }

    $filePath = $meta['result']['file_path'];
    $downloadUrl = 'https://api.telegram.org/file/bot' . $token . '/' . $filePath;
    $content = @file_get_contents($downloadUrl);
    if ($content === false) {
        http_response_code(502);
        echo 'Cannot download file from Telegram';
        exit;
    }

    $name = $file['original_name'] ?: ('file_' . $file['id']);
    header('Content-Description: File Transfer');
    header('Content-Type: application/octet-stream');
    header('Content-Disposition: attachment; filename="' . basename($name) . '"');
    header('Content-Length: ' . strlen($content));
    echo $content;
} catch (Throwable $e) {
    http_response_code(500);
    echo 'Error: ' . $e->getMessage();
}
