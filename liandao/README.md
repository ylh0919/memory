# LIANDAO

-----------------

> `Liandao`是基于[`kungku1.0`](https://github.com/kungfu-origin/kungfu/tree/1.0.0)开源框架修改的低延迟数字货币量化交易系统

------------------

# Setup Liandao on Docker

--------

### Step 1  根据liandao镜像启动docker

```shell
$ systemctl start docker
$ docker images // 查看镜像
```

### Step 1.1 根据镜像创建docker

- [docker菜鸟教程](https://www.runoob.com/docker/docker-container-usage.html) ：想了解基础使用
- [docker hub](https://hub.docker.com/) ：想要寻找某一个模块的docker镜像
- [docker从入门到实践](https://yeasy.gitbook.io/docker_practice/)：了解docker详细使用细节
- [tmux 使用教程](https://www.ruanyifeng.com/blog/2019/10/tmux.html)：一般进入docker操作之前，我们都会进入tmux session, 方便大家一起看

```shell
$ sudo docker run --name md-kungfu-devel -v /home/ec2-user/lbc_docker_share_folder:/shared --privileged --ulimit memlock=-1 --net=host -td taurusai/kungfu-devel /usr/sbin/init
```

- `md-kungfu-devel`**启动的dicker的名字**
- `/home/ec2-user/lbc_docker_share_folder:/shared`docker **内外的文件映射**
- `taurusai/kungfu-devel` **docker images的路径**
- `/usr/sbin/init` **启动镜像后容器里执行的bash**

### Step 1.2 启动并进入刚才创建的docker （先进入tmux）

```shell
$ docker start docker_name
$ docker exec -it testDocker bash
```

--------------------

# Build and Compile

--------------

获取代码（这里是需要`github`权限,还有就是目前需要生成token作为验证方式，[参考链接](https://blog.csdn.net/u014175572/article/details/55510825)

``````shell
git clone https://github.com/beavoinvest/liandao.git
``````

使用 [CMake](https://cmake.org/) 进行编译，CMake 会在当前路径下产生一系列中间文件，建议在单独的编译路径下进行编译：

```shell
$ cd liandao
$ mkdir build
$ cd build
$ cmake ..
$ make [-j4] // 可选选项，多线程编译，第一次编译一般比较慢
$ make package
```

编译成功，会在编译路径下产生 `kungfu-0.0.5-Linux.rpm`，可以使用 yum 进行安装：

```shell
$ yum install kungfu-0.0.1-Linux.rpm
```

安装完成后可以使用`$ kungctl`去验证是否安装成功，成功会进入kungfu的操作界面

如需再次安装，建议先删除已安装版本：

```shell
$ yum erase kungfu
```
