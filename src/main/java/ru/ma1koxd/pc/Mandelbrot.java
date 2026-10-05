package ru.ma1koxd.pc;

import java.awt.image.BufferedImage;
import java.io.File;
import javax.imageio.ImageIO;

public class Mandelbrot {

    // Размер картинки
    static final int WIDTH = 1000;
    static final int HEIGHT = 800;

    // Количество потоков и максимальное количество проверок точки
    static final int THREAD_COUNT = 6;
    static final int MAX_ITERATIONS = 500;

    // Границы участка комплексной плоскости
    static final double MIN_X = -2.0;
    static final double MAX_X = 1.0;
    static final double MIN_Y = -1.2;
    static final double MAX_Y = 1.2;

    public static void main(String[] args) throws Exception {
        // Сначала считаем всё последовательно, одним потоком
        int[] sequentialPixels = new int[WIDTH * HEIGHT];

        long sequentialStart = System.currentTimeMillis();
        calculateRows(0, HEIGHT, sequentialPixels);
        long sequentialEnd = System.currentTimeMillis();

        System.out.println("Последовательный способ: "
                + (sequentialEnd - sequentialStart) + " мс");

        // Теперь считаем то же самое несколькими потоками
        int[] parallelPixels = new int[WIDTH * HEIGHT];
        Thread[] threads = new Thread[THREAD_COUNT];
        int rowsForOneThread = HEIGHT / THREAD_COUNT;

        long parallelStart = System.currentTimeMillis();

        for (int i = 0; i < THREAD_COUNT; i++) {
            final int firstRow = i * rowsForOneThread;
            final int lastRow;

            // Последний поток забирает оставшиеся строки, если они есть
            if (i == THREAD_COUNT - 1) {
                lastRow = HEIGHT;
            } else {
                lastRow = firstRow + rowsForOneThread;
            }

            threads[i] = new Thread(
                    () -> calculateRows(firstRow, lastRow, parallelPixels));
            threads[i].start();
        }

        // Ждём, пока все потоки закончат работу
        for (int i = 0; i < THREAD_COUNT; i++) {
            threads[i].join();
        }

        long parallelEnd = System.currentTimeMillis();

        System.out.println("Параллельный способ: "
                + (parallelEnd - parallelStart) + " мс");

        saveImage(parallelPixels);
        System.out.println("Картинка сохранена в файл mandelbrot.png");
    }

    // Рассчитываем несколько строк картинки
    static void calculateRows(int firstRow, int lastRow, int[] pixels) {
        for (int py = firstRow; py < lastRow; py++) {
            for (int px = 0; px < WIDTH; px++) {
                // Переводим координаты пикселя в координаты комплексного числа
                double x = MIN_X + px * (MAX_X - MIN_X) / (WIDTH - 1);
                double y = MAX_Y - py * (MAX_Y - MIN_Y) / (HEIGHT - 1);

                if (isInMandelbrotSet(x, y)) {
                    pixels[py * WIDTH + px] = 0x000000; // чёрный
                } else {
                    pixels[py * WIDTH + px] = 0xFFFFFF; // белый
                }
            }
        }
    }

    // Проверяем, входит ли точка в множество Мандельброта
    static boolean isInMandelbrotSet(double cx, double cy) {
        double x = 0;
        double y = 0;

        for (int i = 0; i < MAX_ITERATIONS; i++) {
            // Формула z = z * z + c
            double newX = x * x - y * y + cx;
            double newY = 2 * x * y + cy;

            x = newX;
            y = newY;

            // Если расстояние от нуля стало больше 2, точка не входит в множество
            if (x * x + y * y > 4) {
                return false;
            }
        }

        return true;
    }

    // Сохраняем рассчитанные пиксели в PNG-файл
    static void saveImage(int[] pixels) throws Exception {
        BufferedImage image = new BufferedImage(WIDTH, HEIGHT, BufferedImage.TYPE_INT_RGB);
        image.setRGB(0, 0, WIDTH, HEIGHT, pixels, 0, WIDTH);
        ImageIO.write(image, "png", new File("mandelbrot.png"));
    }
}
