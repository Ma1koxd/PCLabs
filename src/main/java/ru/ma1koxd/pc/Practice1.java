package ru.ma1koxd.pc;

public class Practice1 {
    static final int THREADS = 6;
    static final int n = 100000000;
    static final int N_PER_THREAD = n / THREADS;
    static final double l = 0;
    static final double r = 5;
    static final double d = (r - l) / n;

    public static void main(String[] args) throws InterruptedException {
        System.out.println("d = " + d);
        // Последовательный расчет
        long start = System.nanoTime();
        double sum = 0;
        for (int i = 0; i < n; i++) {
            double x = l + d * i;
            sum = sum + f(x) * d;
        }
        long finish = System.nanoTime();
        System.out.println("Последовательный способ");
        System.out.println("Результат: " + sum);
        System.out.println("Точное значение: " + F(r, l));
        System.out.println("Время (мс): " + (double)(finish - start) / 1000000);
        System.out.println();
        executeThreads();
    }

    public static void executeThreads() throws InterruptedException {
        Thread[] threads = new Thread[THREADS];
        double[] results = new double[THREADS];
        long start = System.nanoTime();

        for (int i = 0; i < THREADS; i++) {
            threads[i] = createThread(i, results);
            threads[i].start();
        }

        for (int i = 0; i < THREADS; i++) {
            threads[i].join();
        }

        long finish = System.nanoTime();
        double sum = 0;

        for (int i = 0; i < THREADS; i++) {
            sum = sum + results[i];
        }
        System.out.println("Параллельный способ");
        System.out.println("Результат: " + sum);
        System.out.println("Точное значение: " + F(r, l));
        System.out.println("Время (мс): " + (double)(finish - start) / 1000000);
    }

    public static double f(double x) {
        return x * x * Math.sin(x) + 3 * x;
    }

    public static double F(double r, double l) {
        double right = -r * r * Math.cos(r)
                + 2 * r * Math.sin(r)
                + 2 * Math.cos(r)
                + 3 * r * r / 2;

        double left = -l * l * Math.cos(l)
                + 2 * l * Math.sin(l)
                + 2 * Math.cos(l)
                + 3 * l * l / 2;

        return right - left;
    }

    public static Thread createThread(int number, double[] results) {
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                int start = number * N_PER_THREAD;
                int finish = start + N_PER_THREAD;
                double sum = 0;
                for (int i = start; i < finish; i++) {
                    double x = l + d * i;
                    sum = sum + f(x) * d;
                }
                results[number] = sum;
            }
        });
        return thread;
    }
}