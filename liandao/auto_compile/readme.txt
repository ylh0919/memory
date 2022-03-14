1、在"auto_compile/exchang_select.json"中的select字段自定义需要编译的交易所，这些交易所需要从all字段提供的交易所中选择。其中，CTP和XTP交易所一定会被选择编译(即使该配置文件中没有写)。
2、需要检查"auto_compile/kungfu_sample_template.json"文件，(选择编译的)交易所的md和td的配置是否都存在，如果没有请添加。
3、在编译之前，需要进入"auto_compile"目录，执行"python3 auto_compile.py"命令。如果机器中没有python3环境，需要先执行"yum install python3"命令安装。