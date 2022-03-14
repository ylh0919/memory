#ifndef WINGCHUN_THREAD_POOL_H
#define WINGCHUN_THREAD_POOL_H

#include "WC_DECLARE.h"
#include <vector>
#include <queue>
#include <thread>
#include <atomic>
#include <condition_variable>
#include <future>
#include <functional>
#include "KfLog.h"
WC_NAMESPACE_START

using Task = std::function<void()>;
class TaskPool
{
public:
    // 任务队列
    std::queue<Task> tasks;
    // 同步
    std::mutex mtx;
    // 条件阻塞
    std::condition_variable cv_task;
};
class ThreadPoolOfMultiTask
{
    // 同步
    std::mutex m_lock;
    //任务池
    std::map<int,TaskPool> m_mapTasks;
    std::map<std::string,int> m_mapTaskTypes;
    // 线程池
    std::vector<std::thread> pool;
    // 是否关闭提交
    std::atomic<bool> stoped;
    //最大线程数量
    int  maxThreadNum;
    KfLogPtr logger = yijinjing::KfLog::getLogger("ThreadPoolOfMultiTask");
  public:
      inline ThreadPoolOfMultiTask(unsigned int size = 0) :stoped{ false }
      {
          maxThreadNum = size <  0 ? 0 : size;
          for (size = 0; size < maxThreadNum; ++size)
          {   //初始化线程数量
              auto ptrTaskPool = &(m_mapTasks[size]);
              pool.emplace_back(
                  [this,ptrTaskPool]
                  { // 工作线程函数
                      while(!this->stoped)
                      {
                          std::function<void()> task;
                          {   // 获取一个待执行的 task
                              std::unique_lock<std::mutex> lock{ ptrTaskPool->mtx };// unique_lock 相比 lock_guard 的好处是：可以随时 unlock() 和 lock()
                              ptrTaskPool->cv_task.wait(lock,
                                  [this,ptrTaskPool] {
                                      return this->stoped.load() || !ptrTaskPool->tasks.empty();
                                  }
                              ); // wait 直到有 task
                              if (this->stoped && ptrTaskPool->tasks.empty())
                                  return;
                              task = std::move(ptrTaskPool->tasks.front()); // 取一个 task
                              ptrTaskPool->tasks.pop();
                          }
                          task();
                      }
                  }
              );
          }
      }
      inline ~ThreadPoolOfMultiTask()
      {
          stoped.store(true);
          for(auto& taskPool : m_mapTasks)
          {
              taskPool.second.cv_task.notify_all(); // 唤醒所有线程执行
          }
          for (std::thread& thread : pool) {
              //thread.detach(); // 让线程“自生自灭”
              if(thread.joinable())
                  thread.join(); // 等待任务结束， 前提：线程一定会执行完
          }
      }
  
  public:
      // 提交一个任务
      // 调用.get()获取返回值会等待任务执行完,获取返回值
      // 有两种方法可以实现调用类成员，
      // 一种是使用   bind： .commit(std::bind(&Dog::sayHello, &dog));
      // 一种是用 mem_fn： .commit(std::mem_fn(&Dog::sayHello), &dog)
      template<class F, class... Args>
      void commit(std::string type,F&& f, Args&&... args)
      {
           //KF_LOG_INFO(logger,"[ThreadPool_commit] stoped.load()"<<stoped.load());
          if (stoped.load())    // stop == true ??
              return;
  
          using RetType = decltype(f(args...)); // typename std::result_of<F(Args...)>::type, 函数 f 的返回值类型
          auto task = std::make_shared<std::packaged_task<RetType()> >(
              std::bind(std::forward<F>(f), std::forward<Args>(args)...)
              );    // wtf ! 
          std::future<RetType> future = task->get_future();
          {    // 添加任务到队列
                std::lock_guard<std::mutex> lock{ m_lock };//对当前块的语句加锁  lock_guard 是 mutex 的 stack 封装类，构造的时候 lock()，析构的时候 unlock()
                int thread_id = 0;
                auto it  = m_mapTaskTypes.find(type);
                if(it == m_mapTaskTypes.end())
                {
                    thread_id = m_mapTaskTypes.size() % maxThreadNum;
                    m_mapTaskTypes.insert(std::make_pair(type,thread_id));
                }
                else
                {
                    thread_id = it->second;
                }
                auto& taskPool = m_mapTasks[thread_id];
                {
                    std::lock_guard<std::mutex> lck(taskPool.mtx);
                    taskPool.tasks.emplace(
                        [task]()
                        { // push(Task{...})
                            (*task)();
                        }
                    );
                }
                taskPool.cv_task.notify_one(); // 唤醒一个线程执行
         }
         return;
     }
 };

class ThreadPool
{
    using Task = std::function<void()>;
    // 线程池
    std::vector<std::thread> pool;
    // 任务队列
    std::queue<Task> tasks;
    // 同步
    std::mutex m_lock;
    // 条件阻塞
    std::condition_variable cv_task;
    // 是否关闭提交
    std::atomic<bool> stoped;
      //空闲线程数量
    std::atomic<int>  idlThrNum;
    KfLogPtr logger = yijinjing::KfLog::getLogger("ThreadPool");
  public:
      inline ThreadPool(unsigned int size = 0) :stoped{ false }
      {
          idlThrNum = size <  0 ? 0 : size;
          for (size = 0; size < idlThrNum; ++size)
          {   //初始化线程数量
              pool.emplace_back(
                  [this]
                  { // 工作线程函数
                      while(!this->stoped)
                      {
                          std::function<void()> task;
                          {   // 获取一个待执行的 task
                              std::unique_lock<std::mutex> lock{ this->m_lock };// unique_lock 相比 lock_guard 的好处是：可以随时 unlock() 和 lock()
                              this->cv_task.wait(lock,
                                  [this] {
                                      return this->stoped.load() || !this->tasks.empty();
                                  }
                              ); // wait 直到有 task
                              if (this->stoped && this->tasks.empty())
                                  return;
                              task = std::move(this->tasks.front()); // 取一个 task
                              this->tasks.pop();
                          }
                          idlThrNum--;
                          task();
                          idlThrNum++;
                      }
                  }
              );
          }
      }
      inline ~ThreadPool()
      {
          stoped.store(true);
          cv_task.notify_all(); // 唤醒所有线程执行
          for (std::thread& thread : pool) {
              //thread.detach(); // 让线程“自生自灭”
              if(thread.joinable())
                  thread.join(); // 等待任务结束， 前提：线程一定会执行完
          }
      }
  
  public:
      // 提交一个任务
      // 调用.get()获取返回值会等待任务执行完,获取返回值
      // 有两种方法可以实现调用类成员，
      // 一种是使用   bind： .commit(std::bind(&Dog::sayHello, &dog));
      // 一种是用 mem_fn： .commit(std::mem_fn(&Dog::sayHello), &dog)
      template<class F, class... Args>
      void commit(F&& f, Args&&... args)
      {
           //KF_LOG_INFO(logger,"[ThreadPool_commit] stoped.load()"<<stoped.load());
          if (stoped.load())    // stop == true ??
              return;
  
          using RetType = decltype(f(args...)); // typename std::result_of<F(Args...)>::type, 函数 f 的返回值类型
          auto task = std::make_shared<std::packaged_task<RetType()> >(
              std::bind(std::forward<F>(f), std::forward<Args>(args)...)
              );    // wtf ! 
          std::future<RetType> future = task->get_future();
          {    // 添加任务到队列
              std::lock_guard<std::mutex> lock{ m_lock };//对当前块的语句加锁  lock_guard 是 mutex 的 stack 封装类，构造的时候 lock()，析构的时候 unlock()
              tasks.emplace(
                  [task]()
                 { // push(Task{...})
                     (*task)();
                 }
             );
         }
         cv_task.notify_one(); // 唤醒一个线程执行
 
         return;
     }
 
     //空闲线程数量
     int idlCount() { return idlThrNum; }
 
 };





WC_NAMESPACE_END

#endif //WINGCHUN_THREAD_POOL_H
