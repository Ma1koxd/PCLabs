package ru.ma1koxd.pc;

public class Practice1 {
    static final int THREADS = 6;
    static final int n = 100000000;
    static final int N_PER_THREAD = n / THREADS;
    static final double l = 0;
    static final double r = 4;
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
        return 2 * x;
    }

    public static double F(double r, double l) {
        return r * r - l * l;
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

//Не думал я, что такой результат колоссально отличающийся получится в сравнении с результатом на практике
//"Последовательный способ
//Результат: 15.999999840000022
//Точное значение: 16.0
//Время (мс): 172.7242
//
//Параллельный способ
//Результат: 15.999998560000016
//Точное значение: 16.0
//Время (мс): 59.7719"