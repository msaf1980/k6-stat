# k6-stat

Display statistic for [xk6-output-clickhouse](https://github.com/msaf1980/xk6-output-clickhouse)

Contains two cmds:
* k6-stat-cli  CLI utility
* k6-stat      Web version (TODO)

# Usage

## k6-stat-cli

```
$ ./k6-stat-cli 
k6-stat> 

tests --from 2023-01-17T09:09:21
select -n 0
reference -n 1
diff --out top.txt
```
