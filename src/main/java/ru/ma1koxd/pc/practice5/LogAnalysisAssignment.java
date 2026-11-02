package ru.ma1koxd.pc.practice5;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Random;
import java.io.UncheckedIOException;
import java.nio.file.DirectoryStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/*
 * ПРАКТИЧЕСКОЕ ЗАДАНИЕ: МНОГОПОТОЧНЫЙ АНАЛИЗ ЛОГОВ ВЕБ-СЕРВЕРА
 *
 * Крупные части задания:
 * 1. Сгенерировать набор настоящих файлов с логами.
 * 2. Реализовать разбор строк и последовательный отчёт.
 * 3. Распараллелить обработку нескольких файлов через ExecutorService.
 * 4. Сделать общую статистику потокобезопасной.
 * 5. Ограничить число одновременно читаемых файлов через Semaphore.
 * 6. Добавить Atomic-счётчики, CAS и корректное ожидание завершения.
 * 7. Реализовать досрочную остановку анализа через volatile.
 * 8. Сформулировать и доказать инварианты корректности и отсутствия дедлоков.
 * 9. Реализовать обработку повреждённых файлов и частичный отчёт.
 *
 * ФАЙЛ МОЖНО ЗАПУСТИТЬ САМОСТОЯТЕЛЬНО:
 *   java LogAnalysisAssignment generate logs 20 1000
 *   java LogAnalysisAssignment check
 *
 * Первая команда создаёт каталог logs и файлы access-000.log, access-001.log и т.д.
 * Аргументы: каталог, количество файлов, количество строк в каждом файле.
 * Вторая команда выполняет встроенную проверку генератора и итератора строк.
 *
 * ФОРМАТ СТРОКИ:
 *   METHOD PATH STATUS RESPONSE_BYTES DURATION_MILLIS
 * Например:
 *   GET /api/users 200 820 4
 *
 * Поля разделены одним или несколькими пробелами.
 * Успешный запрос имеет статус от 200 до 399 включительно.
 * Ошибочный запрос имеет статус от 400 и выше.
 *
 * ГОТОВЫЕ СТРУКТУРЫ:
 *   LogEntry       — одна разобранная запись.
 *   LogFile        — путь к одному файлу.
 *   PathStatistics — неизменяемая статистика одного URL.
 *   Report         — итоговый отчёт.
 *   LogFileLines   — итератор строк файла.
 *   GenerateLogs   — генератор входных файлов.
 *
 * ВАЖНО ПРО LogFileLines:
 * Итератор потоконебезопасен. Один экземпляр Iterator<String> нельзя читать
 * из нескольких потоков одновременно. Каждый поток должен получить собственный
 * итератор для своего файла. Нельзя разделить один итератор между задачами.
 * Сам файл можно читать параллельно разными потоками только при условии, что
 * каждый поток создаёт отдельный итератор.
 *
 * ЧАСТИ ЗАДАНИЯ:
 *
 * 1. РАЗБОР СТРОКИ
 * Реализуйте преобразование одной строки в LogEntry. Проверьте количество
 * полей, преобразуйте числовые значения и выбрасывайте понятную ошибку для
 * повреждённой строки.
 *
 * 2. ПОСЛЕДОВАТЕЛЬНЫЙ ОТЧЁТ
 * Напишите обработку всех файлов одним потоком. Для каждого URL собирайте
 * количество запросов, количество успешных запросов и ошибок, объём ответов,
 * суммарное и максимальное время обработки. Этот вариант станет эталоном для
 * проверки многопоточной версии.
 *
 * 3. ПАРАЛЛЕЛЬНАЯ ОБРАБОТКА ФАЙЛОВ
 * Создайте фиксированный ExecutorService. Каждый файл обрабатывается отдельной
 * задачей. Рабочий поток создаёт собственный LogFileLines.Iterator, читает свой
 * файл и передаёт записи в общий аккумулятор. Главный поток должен дождаться
 * завершения всех файлов и только потом построить Report.
 *
 * 4. ПОТОКОБЕЗОПАСНАЯ СТАТИСТИКА
 * Реализуйте аккумулятор статистики. Сначала допустимо использовать один общий
 * synchronized-метод. Затем реализуйте вариант с отдельным synchronized-аккумулятором
 * для каждого URL и ConcurrentHashMap. Сравните эти варианты на данных, где почти
 * все запросы относятся к одному URL, и на данных с большим числом URL.
 *
 * 5. ОГРАНИЧЕНИЕ ЧТЕНИЯ ФАЙЛОВ
 * Добавьте Semaphore, ограничивающий число файлов, которые могут обрабатываться
 * одновременно. Рабочих потоков может быть больше разрешений семафора. Разрешение
 * нужно получить перед чтением файла и гарантированно вернуть после завершения.
 * Добавьте наблюдение за текущим и максимальным числом активных файлов.
 *
 * 6. ATOMIC-СЧЁТЧИКИ И CAS
 * Добавьте счётчики обработанных файлов, прочитанных строк, корректных записей,
 * повреждённых записей, ошибок чтения и активных файлов. Для максимального числа
 * активных файлов используйте AtomicInteger и compareAndSet. Обычные изменяемые
 * int/long для этих глобальных счётчиков не используйте.
 *
 * 7. ОЖИДАНИЕ ЗАВЕРШЕНИЯ
 * Организуйте ожидание всех файлов через CountDownLatch либо через набор Future.
 * Завершение задачи должно учитываться даже при исключении. ExecutorService нужно
 * корректно закрыть и дождаться его завершения.
 *
 * CountDownLatch — это одноразовый счётчик-сигнал. Его создают с начальным
 * числом, например равным количеству файлов. Главный поток вызывает await()
 * и блокируется, пока счётчик не станет равен нулю. Каждая завершившаяся задача
 * один раз вызывает countDown(), уменьшая счётчик. Когда последний счётчик
 * уменьшен до нуля, все ожидающие потоки продолжают работу. Увеличить latch
 * обратно или использовать его повторно нельзя, поэтому для нового запуска
 * создаётся новый объект. Вызов countDown() нужно помещать в finally, чтобы
 * исключение в задаче не оставило главный поток ждать бесконечно.
 *
 * 8. ДОСРОЧНАЯ ОСТАНОВКА
 * Добавьте volatile-флаг остановки. После остановки новые строки не начинают
 * обрабатываться, а уже начатая операция может завершиться. Опишите, какие данные
 * входят в частичный отчёт. Не оставляйте рабочие потоки и разрешения семафора
 * в подвешенном состоянии.
 *
 * 9. ПОВРЕЖДЁННЫЕ ФАЙЛЫ И ЧАСТИЧНЫЙ ОТЧЁТ
 * Сделайте так, чтобы ошибка одного файла не отменяла обработку остальных файлов.
 * В итоговом результате отдельно храните список или число файлов, которые не удалось
 * обработать. Решите, что происходит при повреждённой строке: прекращается обработка
 * всего файла или пропускается только эта строка. Выберите один вариант и явно
 * зафиксируйте его в документации. Для полного запуска корректные файлы должны
 * давать полный отчёт, а ошибка должна быть видна пользователю.
 *
 * ИНВАРИАНТЫ, КОТОРЫЕ НУЖНО ДОКАЗАТЬ:
 * - каждая строка полного запуска обработана ровно один раз;
 * - для каждого URL requests = successful + errors;
 * - maxDuration <= totalDuration при наличии запросов;
 * - 0 <= activeFiles <= semaphoreLimit;
 * - после завершения анализа activeFiles равно нулю;
 * - один повреждённый файл не мешает обработке остальных;
 * - в программе нет дедлока.
 *
 * В доказательстве отсутствия дедлока укажите порядок захвата мониторов и
 * семафоров. Отдельно объясните, может ли поток ждать acquire(), удерживая
 * synchronized-монитор, и почему разрешение семафора возвращается при исключении.
 * Объясните, почему volatile нужен для флага остановки.
 *
 * НЕБОЛЬШИЕ ЭКСПЕРИМЕНТЫ ПОСЛЕ РЕАЛИЗАЦИИ:
 * - сравнить 1 и 4 рабочих потока;
 * - сравнить лимит семафора 1 и 4;
 * - сравнить глобальную блокировку и блокировки по URL;
 * - добавить одну повреждённую строку или один повреждённый файл и проверить,
 *   что остальные данные всё равно попадают в отчёт.
 * Эксперименты не требуют дополнительной архитектуры или новых классов.
 */
public class LogAnalysisAssignment {

    public record LogEntry(
            String method,
            String path,
            int status,
            long responseBytes,
            long durationMillis
    ) {}

    public record LogFile(Path path) {}

    public record PathStatistics(
            long requests,
            long successful,
            long errors,
            long responseBytes,
            long totalDurationMillis,
            long maxDurationMillis
    ) {}

    public record Report(
            long totalRequests,
            long successfulRequests,
            long errorRequests,
            long responseBytes,
            long maxDurationMillis,
            long totalDurationMillis,
            Map<String, PathStatistics> byPath,
            long readLines,
            long validRecords,
            long badRecords,
            int processedFiles,
            int failedFiles,
            int cancelledFiles,
            int activeFiles,
            int maxActiveFiles,
            boolean stopped
    ) {}

    public static LogEntry parseLine(String line) {
        if (line == null) {
            throw new IllegalArgumentException("Строка отсутствует");
        }

        String[] fields = line.trim().split("\\s+");

        if (fields.length != 5) {
            throw new IllegalArgumentException("В строке должно быть 5 полей");
        }

        int status;
        long bytes;
        long duration;

        try {
            status = Integer.parseInt(fields[2]);
            bytes = Long.parseLong(fields[3]);
            duration = Long.parseLong(fields[4]);
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException("Неправильное число в строке", exception);
        }

        // В задании рассматриваются статусы от 200.
        if (status < 200) {
            throw new IllegalArgumentException("Статус должен быть не меньше 200");
        }

        if (bytes < 0 || duration < 0) {
            throw new IllegalArgumentException("Размер и время не могут быть отрицательными");
        }

        return new LogEntry(fields[0], fields[1], status, bytes, duration);
    }

    public static class MutableStatistics {
        private long requests;
        private long successful;
        private long errors;
        private long bytes;
        private long totalDuration;
        private long maxDuration;

        public synchronized void add(LogEntry entry) {
            requests++;

            if (entry.status() <= 399) {
                successful++;
            } else {
                errors++;
            }

            bytes += entry.responseBytes();
            totalDuration += entry.durationMillis();

            if (entry.durationMillis() > maxDuration) {
                maxDuration = entry.durationMillis();
            }
        }

        public synchronized PathStatistics snapshot() {
            return new PathStatistics(requests, successful, errors, bytes, totalDuration, maxDuration);
        }
    }

    public static class Analyzer {
        /*
         * ПРАВИЛА ОБРАБОТКИ ОШИБОК И ОСТАНОВКИ
         *
         * Повреждённая строка пропускается. Остальные строки файла
         * продолжают обрабатываться. Число таких строк хранится в badRecords.
         *
         * Ошибка открытия, чтения или закрытия файла учитывается в failedFiles.
         * Уже собранная статистика сохраняется, остальные файлы обрабатываются.
         *
         * При остановке в частичный отчёт входят корректные записи,
         * обработанные до завершения рабочих задач. Уже начатая операция
         * со строкой может завершиться. Непрочитанные строки не учитываются.
         *
         * ОБОСНОВАНИЕ ИНВАРИАНТОВ
         *
         * 1. Каждая строка полного запуска обрабатывается ровно один раз.
         * findFiles добавляет каждый файл один раз. Для файла создаётся одна
         * задача со своим итератором. Итератор последовательно выдаёт строки,
         * каждая из которых учитывается как корректная или повреждённая.
         * Полным здесь считается запуск без остановки и ошибок чтения.
         *
         * 2. requests = successful + errors.
         * При добавлении записи увеличивается requests и ровно один
         * из счётчиков successful или errors. Изменения защищены synchronized.
         *
         * 3. maxDuration <= totalDuration.
         * Время каждой записи неотрицательное, поэтому сумма времён
         * не меньше максимального времени. Предполагается, что суммы
         * помещаются в long.
         *
         * 4. 0 <= activeFiles <= semaphoreLimit.
         * Счётчик увеличивается только после получения разрешения.
         * Уменьшение выполняется до возврата разрешения.
         * Число активных файлов не может превысить число разрешений.
         *
         * 5. После завершения activeFiles равно нулю.
         * Каждому увеличению соответствует уменьшение в finally.
         * Отчёт строится только после завершения всех задач.
         *
         * 6. Ошибка одного файла не мешает другим.
         * Ошибки ввода-вывода перехватываются внутри processFile.
         * Разрешение возвращается в finally. Задача в любом случае
         * вызывает countDown в своём finally.
         *
         * 7. Дедлока нет.
         * Сначала получается разрешение семафора.
         * При общей блокировке затем берётся монитор Analyzer,
         * после него — монитор MutableStatistics.
         * При блокировках по URL операции с картой завершаются
         * до захвата монитора соответствующего MutableStatistics.
         * Обратного порядка захвата этих мониторов нет.
         *
         * acquire не вызывается с удержанием synchronized-монитора.
         * Главный поток также не удерживает монитор при ожидании задач.
         * Разрешение семафора возвращается в finally даже при исключении,
         * если оно было получено. Файл закрывается через try с ресурсом.
         * Поэтому кругового ожидания нет.
         * Предполагается, что файловые операции в итоге завершаются.
         *
         * 8. volatile обеспечивает видимость изменения stopRequested
         * другим потокам. Для числовых счётчиков используются Atomic-классы,
         * поскольку volatile не делает операцию увеличения атомарной.
         */

        private final ConcurrentHashMap<String, MutableStatistics> statistics =
                new ConcurrentHashMap<>();

        private final Semaphore semaphore;
        private final boolean globalLock;

        private final AtomicInteger processedFiles = new AtomicInteger();
        private final AtomicInteger failedFiles = new AtomicInteger();
        private final AtomicInteger cancelledFiles = new AtomicInteger();

        private final AtomicLong readLines = new AtomicLong();
        private final AtomicLong validRecords = new AtomicLong();
        private final AtomicLong badRecords = new AtomicLong();

        private final AtomicInteger activeFiles = new AtomicInteger();
        private final AtomicInteger maxActiveFiles = new AtomicInteger();

        private volatile boolean stopRequested;

        public Analyzer(int semaphoreLimit, boolean globalLock) {
            if (semaphoreLimit < 1) {
                throw new IllegalArgumentException(
                        "Лимит семафора должен быть положительным"
                );
            }

            semaphore = new Semaphore(semaphoreLimit);
            this.globalLock = globalLock;
        }

        public void requestStop() {
            stopRequested = true;
        }

        // Первый вариант: общая блокировка для всех URL.
        private synchronized void addWithGlobalLock(LogEntry entry) {
            MutableStatistics current = statistics.get(entry.path());

            if (current == null) {
                current = new MutableStatistics();
                statistics.put(entry.path(), current);
            }

            current.add(entry);
        }

        // Второй вариант: отдельная блокировка для каждого URL.
        private void addEntry(LogEntry entry) {
            if (globalLock) {
                addWithGlobalLock(entry);
                return;
            }

            MutableStatistics current = statistics.get(entry.path());

            if (current == null) {
                MutableStatistics newStatistics = new MutableStatistics();

                // Если другой поток уже добавил этот URL,
                // используем его аккумулятор.
                MutableStatistics previous = statistics.putIfAbsent(
                        entry.path(),
                        newStatistics
                );

                if (previous == null) {
                    current = newStatistics;
                } else {
                    current = previous;
                }
            }

            current.add(entry);
        }

        // Обновление максимума через CAS.
        private void updateMaximum(int current) {
            int oldMaximum = maxActiveFiles.get();

            while (current > oldMaximum) {
                if (maxActiveFiles.compareAndSet(oldMaximum, current)) {
                    return;
                }

                oldMaximum = maxActiveFiles.get();
            }
        }

        // Обработка одного файла.
        private void processFile(LogFile file) {
            boolean permitTaken = false;
            boolean active = false;
            boolean completelyRead = false;

            try {
                if (stopRequested || Thread.currentThread().isInterrupted()) {
                    cancelledFiles.incrementAndGet();
                    return;
                }

                semaphore.acquire();
                permitTaken = true;

                if (stopRequested || Thread.currentThread().isInterrupted()) {
                    cancelledFiles.incrementAndGet();
                    return;
                }

                int current = activeFiles.incrementAndGet();
                active = true;
                updateMaximum(current);

                // У каждого файла собственный итератор.
                // Файл закроется при выходе из этого блока.
                try (LogFileLines lines = LogFileLines.iterator(file.path())) {
                    while (!stopRequested
                            && !Thread.currentThread().isInterrupted()) {

                        if (!lines.hasNext()) {
                            completelyRead = true;
                            break;
                        }

                        String line = lines.next();
                        readLines.incrementAndGet();

                        LogEntry entry;

                        try {
                            entry = parseLine(line);
                        } catch (IllegalArgumentException exception) {
                            // Пропускаем только неправильную строку.
                            badRecords.incrementAndGet();
                            continue;
                        }

                        addEntry(entry);
                        validRecords.incrementAndGet();
                    }
                }

                if (completelyRead) {
                    processedFiles.incrementAndGet();
                } else {
                    cancelledFiles.incrementAndGet();
                }

            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                cancelledFiles.incrementAndGet();

            } catch (IOException | UncheckedIOException exception) {
                failedFiles.incrementAndGet();

                System.err.println(
                        "Ошибка файла " + file.path()
                                + ": " + exception.getMessage()
                );

            } finally {
                if (active) {
                    activeFiles.decrementAndGet();
                }

                if (permitTaken) {
                    semaphore.release();
                }
            }
        }

        // Последовательная обработка.
        public Report analyzeSequential(List<LogFile> files) {
            for (LogFile file : files) {
                processFile(file);
            }

            return buildReport();
        }

        // Параллельная обработка.
        public Report analyzeParallel(List<LogFile> files, int threads) throws InterruptedException {
            if (threads < 1) {
                throw new IllegalArgumentException(
                        "Число потоков должно быть положительным"
                );
            }

            ExecutorService executor =
                    Executors.newFixedThreadPool(threads);

            CountDownLatch latch = new CountDownLatch(files.size());
            boolean interrupted = false;

            try {
                for (LogFile file : files) {
                    executor.execute(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                processFile(file);
                            } finally {
                                latch.countDown();
                            }
                        }
                    });
                }

                executor.shutdown();
                latch.await();

            } catch (InterruptedException exception) {
                requestStop();
                interrupted = true;

            } finally {
                executor.shutdown();

                // Даже при прерывании ждём завершения задач.
                while (!executor.isTerminated()) {
                    try {
                        executor.awaitTermination(1, TimeUnit.SECONDS);
                    } catch (InterruptedException exception) {
                        requestStop();
                        interrupted = true;
                    }
                }
            }

            if (interrupted) {
                Thread.currentThread().interrupt();
                throw new InterruptedException("Ожидание анализа было прервано");
            }

            return buildReport();
        }

        // Собираем отчёт после завершения обработки.
        private Report buildReport() {
            Map<String, PathStatistics> result = new TreeMap<>();

            long requests = 0;
            long successful = 0;
            long errors = 0;
            long bytes = 0;
            long totalDuration = 0;
            long maxDuration = 0;

            for (Map.Entry<String, MutableStatistics> item : statistics.entrySet()) {
                PathStatistics value = item.getValue().snapshot();
                result.put(item.getKey(), value);

                requests += value.requests();
                successful += value.successful();
                errors += value.errors();
                bytes += value.responseBytes();
                totalDuration += value.totalDurationMillis();

                if (value.maxDurationMillis() > maxDuration) {
                    maxDuration = value.maxDurationMillis();
                }
            }

            return new Report(
                    requests,
                    successful,
                    errors,
                    bytes,
                    maxDuration,
                    totalDuration,
                    Collections.unmodifiableMap(result),
                    readLines.get(),
                    validRecords.get(),
                    badRecords.get(),
                    processedFiles.get(),
                    failedFiles.get(),
                    cancelledFiles.get(),
                    activeFiles.get(),
                    maxActiveFiles.get(),
                    stopRequested
            );
        }
    }

    /**
     * Возвращает отдельный итератор для чтения строк одного файла.
     *
     * ВАЖНО: Iterator<String> потоконебезопасен. Один экземпляр нельзя
     * одновременно читать из нескольких потоков. Для каждого потока нужен
     * отдельный вызов LogFileLines.iterator(path).
     */
    public static final class LogFileLines implements Iterator<String>, AutoCloseable {

        private final BufferedReader reader;

        private String nextLine;
        private boolean lineReady;
        private boolean closed;

        private LogFileLines(Path path) throws IOException {
            reader = Files.newBufferedReader(
                    path,
                    StandardCharsets.UTF_8
            );
        }

        public static LogFileLines iterator(Path path) throws IOException {
            return new LogFileLines(path);
        }

        @Override
        public boolean hasNext() {
            if (closed) {
                return false;
            }
            if (!lineReady) {
                try {
                    nextLine = reader.readLine();

                    if (nextLine == null) {
                        close();
                        return false;
                    }

                    lineReady = true;
                } catch (IOException exception) {
                    throw new UncheckedIOException("Не удалось прочитать строку", exception);
                }
            }

            return true;
        }

        @Override
        public String next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            String result = nextLine;

            nextLine = null;
            lineReady = false;

            return result;
        }

        @Override
        public void close() throws IOException {
            if (!closed) {
                closed = true;
                reader.close();
            }
        }
    }

    /** Генерирует реальные текстовые файлы с детерминированными данными. */
    public static final class GenerateLogs {
        private static final String[] METHODS = {
                "GET", "POST", "PUT", "DELETE"
        };

        private static final String[] PATHS = {
                "/", "/index.html", "/login", "/api/users",
                "/api/orders", "/api/products", "/search", "/health"
        };

        private static final int[] STATUSES = {
                200, 200, 200, 201, 204,
                301, 400, 401, 404, 500
        };

        private GenerateLogs() {}

        public static void generate(
                Path directory,
                int fileCount,
                int linesPerFile
        ) throws IOException {
            generate(directory, fileCount, linesPerFile, PATHS.length);
        }

        public static void generate(Path directory, int fileCount, int linesPerFile, int pathCount) throws IOException {
            if (fileCount < 0 || linesPerFile < 0 || pathCount < 1) {
                throw new IllegalArgumentException("Неправильные параметры генерации");
            }

            Files.createDirectories(directory);
            Random random = new Random(42);

            for (int fileId = 0; fileId < fileCount; fileId++) {
                Path file = directory.resolve(
                        String.format("access-%03d.log", fileId)
                );

                try (BufferedWriter writer = Files.newBufferedWriter(
                        file,
                        StandardCharsets.UTF_8
                )) {
                    for (int line = 0; line < linesPerFile; line++) {
                        String method = METHODS[random.nextInt(METHODS.length)];

                        int pathId = random.nextInt(pathCount);
                        String path;

                        if (pathId < PATHS.length) {
                            path = PATHS[pathId];
                        } else {
                            path = "/page/" + pathId;
                        }

                        int status = STATUSES[random.nextInt(STATUSES.length)];

                        long bytes = 100L + random.nextInt(100_000);
                        long duration = 1L + random.nextInt(500);

                        writer.write(
                                method + " " + path + " " + status
                                        + " " + bytes + " " + duration
                        );

                        writer.newLine();
                    }
                }
            }
        }
    }

    /** Проверяет генератор и чтение строк без участия студенческого анализатора. */
    private static List<LogFile> findFiles(Path directory) throws IOException {
        List<LogFile> files = new ArrayList<>();

        try (DirectoryStream<Path> paths =
                     Files.newDirectoryStream(directory, "*.log")) {
            for (Path path : paths) {
                if (Files.isRegularFile(path)) {
                    files.add(new LogFile(path));
                }
            }
        }

        return files;
    }

    private static void printReport(Report report) {
        System.out.println(
                "\nURL | запросы | успешные | ошибки | байты | время | максимум"
        );

        for (Map.Entry<String, PathStatistics> item : report.byPath().entrySet()) {
            PathStatistics s = item.getValue();

            System.out.println(
                    item.getKey()
                            + " | " + s.requests()
                            + " | " + s.successful()
                            + " | " + s.errors()
                            + " | " + s.responseBytes()
                            + " | " + s.totalDurationMillis()
                            + " | " + s.maxDurationMillis()
            );
        }

        System.out.println("Всего запросов: " + report.totalRequests());
        System.out.println("Успешных: " + report.successfulRequests());
        System.out.println("Ошибочных: " + report.errorRequests());
        System.out.println("Байт: " + report.responseBytes());

        System.out.println("Суммарное время, мс: " + report.totalDurationMillis());
        System.out.println("Максимальное время, мс: " + report.maxDurationMillis());

        System.out.println("Прочитано строк: " + report.readLines());
        System.out.println("Корректных записей: " + report.validRecords());
        System.out.println("Повреждённых записей: " + report.badRecords());

        System.out.println("Полностью прочитано файлов: " + report.processedFiles());
        System.out.println("Файлов с ошибкой чтения: " + report.failedFiles());
        System.out.println("Отменённых файлов: " + report.cancelledFiles());
        System.out.println("Активных файлов сейчас: " + report.activeFiles());
        System.out.println("Максимум активных файлов: " + report.maxActiveFiles());
        System.out.println("Запрошена остановка: " + report.stopped());
    }

    private static boolean sameData(Report a, Report b) {
        return a.byPath().equals(b.byPath())
                && a.readLines() == b.readLines()
                && a.validRecords() == b.validRecords()
                && a.badRecords() == b.badRecords()
                && a.processedFiles() == b.processedFiles()
                && a.failedFiles() == b.failedFiles()
                && a.cancelledFiles() == b.cancelledFiles();
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    private static void checkInvariants(
            Report report,
            int fileCount,
            int limit
    ) {
        require(report.readLines() == report.validRecords() + report.badRecords(), "Неверные счётчики строк");
        require(report.totalRequests() == report.validRecords(), "Потеряны записи");
        require(report.totalRequests() == report.successfulRequests() + report.errorRequests(), "Неверный общий счётчик запросов");
        require(report.activeFiles() == 0, "Остались активные файлы");
        require(report.maxActiveFiles() >= 0 && report.maxActiveFiles() <= limit, "Нарушен лимит семафора");
        require(report.processedFiles() + report.failedFiles() + report.cancelledFiles() == fileCount, "Не все файлы учтены");

        for (PathStatistics s : report.byPath().values()) {
            require(s.requests() == s.successful() + s.errors(), "Неверная статистика URL");
            require(s.maxDurationMillis() <= s.totalDurationMillis(), "Неверный максимум");
        }
    }

    private static void selfCheck() throws Exception {
        Path directory = Files.createTempDirectory("log-assignment-");

        try {
            GenerateLogs.generate(directory, 3, 7);

            List<LogFile> files = findFiles(directory);

            Report sequential = new Analyzer(1, true).analyzeSequential(files);

            require(sequential.readLines() == 21, "Генератор должен создать 21 строку");

            // Проверяем оба варианта блокировки.
            for (boolean global : new boolean[]{true, false}) {
                Report parallel = new Analyzer(2, global).analyzeParallel(files, 4);

                require(sameData(sequential, parallel), "Отчёты отличаются");

                checkInvariants(parallel, 3, 2);
            }

            // Создаём файл с одной повреждённой строкой.
            Path damaged = directory.resolve("damaged.log");

            Files.writeString(
                    damaged,
                    "GET /test 200 10 5\n"
                            + "сломанная строка\n"
                            + "POST /test 500 20 8\n"
                            + "GET /test 301 0 0\n",
                    StandardCharsets.UTF_8
            );

            List<LogFile> damagedFiles = new ArrayList<>();

            damagedFiles.add(new LogFile(damaged));

            // Этого файла нет: проверяем ошибку открытия.
            damagedFiles.add(new LogFile(directory.resolve("missing.log")));

            for (boolean global : new boolean[]{true, false}) {
                Report partial = new Analyzer(1, global).analyzeParallel(damagedFiles, 4);

                PathStatistics expected = new PathStatistics(3, 2, 1, 30, 13, 8);

                require(expected.equals(partial.byPath().get("/test")), "Неверная сумма");
                require(partial.badRecords() == 1 && partial.failedFiles() == 1, "Ошибки должны попасть в отчёт"
                );

                checkInvariants(partial, 2, 1);
            }

            // Проверяем остановку до начала чтения.
            Analyzer stopped = new Analyzer(1, false);
            stopped.requestStop();

            Report partial = stopped.analyzeParallel(files, 4);

            require(partial.stopped() && partial.readLines() == 0 && partial.cancelledFiles() == 3, "Остановка не сработала");

            checkInvariants(partial, 3, 1);

            System.out.println("OK: генератор, блокировки, ошибки и остановка");

        } finally {
            // Удаляем файлы временной проверки.
            try (DirectoryStream<Path> paths = Files.newDirectoryStream(directory)) {
                for (Path path : paths) {
                    Files.delete(path);
                }
            }

            Files.delete(directory);
        }
    }

    private static void experiment(Path directory) throws Exception {
        for (int pathCount : new int[]{1, 1000}) {
            Path data = directory.resolve("urls-" + pathCount);

            GenerateLogs.generate(data, 8, 10_000, pathCount);

            List<LogFile> files = findFiles(data);

            Report sequential = new Analyzer(1, true).analyzeSequential(files);

            System.out.println("\nЧисло URL: " + pathCount);

            System.out.println("Потоки | лимит | блокировка | время, мс | совпадает");

            for (int threads : new int[]{1, 4}) {
                for (int limit : new int[]{1, 4}) {
                    for (boolean global : new boolean[]{true, false}) {
                        long start = System.nanoTime();

                        Report report = new Analyzer(limit, global).analyzeParallel(files, threads);

                        long elapsed = (System.nanoTime() - start) / 1_000_000;

                        String lock;

                        if (global) {
                            lock = "общая";
                        } else {
                            lock = "по URL";
                        }

                        boolean same = sameData(sequential, report);

                        System.out.println(
                                threads + " | " + limit
                                        + " | " + lock
                                        + " | " + elapsed
                                        + " | " + same
                        );

                        require(same, "Результат отличается от последовательного");

                        checkInvariants(report, files.size(), limit);
                    }
                }
            }
        }
    }

    private static Report analyzeWithStop(
            List<LogFile> files,
            long delay
    ) throws InterruptedException {
        if (delay < 0) {
            throw new IllegalArgumentException("Задержка не может быть отрицательной");
        }

        Analyzer analyzer = new Analyzer(2, false);

        Thread stopper = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(delay);
                    analyzer.requestStop();
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                }
            }
        });

        stopper.start();

        try {
            return analyzer.analyzeParallel(files, 4);
        } finally {
            stopper.interrupt();
            stopper.join();
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            // 1. Создаём файлы для анализа.
            System.out.println("1. Создание 20 файлов по 1000 строк");

            Path demoDirectory = Path.of("logs");

            GenerateLogs.generate(demoDirectory, 20, 1000);

            List<LogFile> files = findFiles(demoDirectory);

            // 2. Обрабатываем файлы одним потоком.
            System.out.println("\n2. Последовательный анализ");

            Analyzer sequentialAnalyzer = new Analyzer(1, true);

            Report sequential = sequentialAnalyzer.analyzeSequential(files);

            printReport(sequential);
            checkInvariants(sequential, files.size(), 1);

            // 3. Обрабатываем те же файлы несколькими потоками.
            System.out.println("\n3. Параллельный анализ: 4 потока, лимит 2");

            Analyzer parallelAnalyzer = new Analyzer(2, false);

            Report parallel = parallelAnalyzer.analyzeParallel(files, 4);

            printReport(parallel);
            checkInvariants(parallel, files.size(), 2);

            // Сравниваем полученные данные.
            boolean same = sameData(sequential, parallel);

            System.out.println("\nОтчёты совпадают: " + same);

            require(same, "Последовательный и параллельный отчёты отличаются");

            // 4. Сравниваем количество потоков, лимиты и блокировки.
            System.out.println("\n4. Сравнение потоков, семафора и блокировок");

            Path experimentDirectory = Path.of("experiment-logs");

            experiment(experimentDirectory);

            // 5. Проверяем повреждённые данные.
            System.out.println("\n5. Проверка повреждённых данных и остановки");
            System.out.println("Ошибки missing.log в этой проверке создаются специально.");

            selfCheck();

            // 6. Показываем частичный отчёт при остановке.
            System.out.println("\n6. Досрочная остановка во время анализа");

            List<LogFile> stopFiles = findFiles(experimentDirectory.resolve("urls-1"));

            Report partial = analyzeWithStop(stopFiles, 10);

            printReport(partial);
            checkInvariants(partial, stopFiles.size(), 2);

            System.out.println("\nВсе этапы завершены.");

            return;
        }

        if (args[0].equals("check")) {
            selfCheck();
            return;
        }

        Path directory = Path.of("logs");

        if (args.length > 1) {
            directory = Path.of(args[1]);
        }

        if (args[0].equals("generate")) {
            int fileCount = 20;
            int linesPerFile = 1000;

            if (args.length > 2) {
                fileCount = Integer.parseInt(args[2]);
            }

            if (args.length > 3) {
                linesPerFile = Integer.parseInt(args[3]);
            }

            GenerateLogs.generate(directory, fileCount, linesPerFile);

            System.out.println("Создано файлов: " + fileCount + ", каталог: " + directory);

        } else if (args[0].equals("experiment")) {
            experiment(directory);

        } else if (args[0].equals("sequential")) {
            Analyzer analyzer = new Analyzer(1, true);

            Report report = analyzer.analyzeSequential(findFiles(directory));

            printReport(report);

        } else if (args[0].equals("parallel")) {
            int threads = 4;
            int limit = 2;
            boolean global = false;

            if (args.length > 2) {
                threads = Integer.parseInt(args[2]);
            }

            if (args.length > 3) {
                limit = Integer.parseInt(args[3]);
            }

            if (args.length > 4) {
                if (!args[4].equals("global") && !args[4].equals("url")) {

                    throw new IllegalArgumentException(
                            "Укажите global или url"
                    );
                }

                global = args[4].equals("global");
            }

            Analyzer analyzer = new Analyzer(limit, global);

            Report report = analyzer.analyzeParallel(findFiles(directory), threads);

            printReport(report);

        } else if (args[0].equals("stop")) {
            long delay = 50;

            if (args.length > 2) {
                delay = Long.parseLong(args[2]);
            }

            Report report = analyzeWithStop(findFiles(directory), delay);

            printReport(report);

        } else {
            System.out.println("Неизвестная команда. " + "Запустите программу без аргументов для справки.");
        }
    }
}
