package org.apache.flink.cep.coordinator;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.FatalExitExceptionHandler;

import javax.annotation.Nullable;
import java.util.concurrent.ThreadFactory;

/**
 * 协调器执行线程工厂类。
 *
 * <p>该类实现了 {@link ThreadFactory} 和 {@link Thread.UncaughtExceptionHandler} 接口，
 * 用于创建协调器线程，并提供一些辅助方法。
 *
 * <p>协调器线程工厂设置了自定义的线程上下文类加载器和异常处理机制，确保在异常发生时能够正确处理。
 *
 * @see ThreadFactory
 * @see Thread.UncaughtExceptionHandler
 */
public class CoordinatorExecutorThreadFactory
        implements ThreadFactory, Thread.UncaughtExceptionHandler {

    // 协调器线程的名称
    private final String coordinatorThreadName;

    // 协调器线程的上下文类加载器
    private final ClassLoader classLoader;

    // 未捕获异常的处理器
    private final Thread.UncaughtExceptionHandler errorHandler;

    // 当前线程对象，可能为空
    @Nullable
    private Thread thread;
    // TODO discuss if we should fail the job(JM may restart the job later) or directly kill JM
    // process
    // Currently we choose to directly kill JM process
    /**
     * 构造函数，用于初始化协调器线程工厂。
     *
     * <p>默认使用 {@link FatalExitExceptionHandler} 作为未捕获异常的处理器。
     * 当发生未捕获异常时，直接退出作业管理器 (JM) 进程。
     *
     * @param coordinatorThreadName 协调器线程的名称
     * @param contextClassLoader 协调器线程的上下文类加载器
     */
    CoordinatorExecutorThreadFactory(
            final String coordinatorThreadName, final ClassLoader contextClassLoader) {
        this(coordinatorThreadName, contextClassLoader, FatalExitExceptionHandler.INSTANCE);
    }

    /**
     * 构造函数，用于测试，允许指定自定义的未捕获异常处理器。
     *
     * @param coordinatorThreadName 协调器线程的名称
     * @param contextClassLoader 协调器线程的上下文类加载器
     * @param errorHandler 自定义的未捕获异常处理器
     */
    @VisibleForTesting
    CoordinatorExecutorThreadFactory(
            final String coordinatorThreadName,
            final ClassLoader contextClassLoader,
            final Thread.UncaughtExceptionHandler errorHandler) {
        this.coordinatorThreadName = coordinatorThreadName;
        this.classLoader = contextClassLoader;
        this.errorHandler = errorHandler;
    }

    /**
     * 创建一个新的线程。
     *
     * <p>该线程会使用提供的 Runnable 执行任务，并设置自定义的线程名称、类加载器和未捕获异常处理器。
     *
     * @param r 要执行的任务
     * @return 创建的线程
     */
    @Override
    public synchronized Thread newThread(Runnable r) {
        thread = new Thread(r, coordinatorThreadName); // 创建线程并设置名称
        thread.setContextClassLoader(classLoader); // 设置上下文类加载器
        thread.setUncaughtExceptionHandler(this); // 设置未捕获异常处理器
        return thread;
    }

    /**
     * 处理线程的未捕获异常。
     *
     * <p>当线程运行过程中出现未捕获的异常时，会调用提供的异常处理器进行处理。
     *
     * @param t 发生异常的线程
     * @param e 异常对象
     */
    @Override
    public synchronized void uncaughtException(Thread t, Throwable e) {
        errorHandler.uncaughtException(t, e);
    }

    /**
     * 获取协调器线程的名称。
     *
     * @return 协调器线程的名称
     */
    public String getCoordinatorThreadName() {
        return coordinatorThreadName;
    }

    /**
     * 检查当前线程是否为协调器线程。
     *
     * @return 如果当前线程是协调器线程，则返回 true；否则返回 false
     */
    boolean isCurrentThreadCoordinatorThread() {
        return Thread.currentThread() == thread;
    }
}

