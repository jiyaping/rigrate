# Rigrate

## Overview
This gem is used to migrate data between different data source in an easy way. SQLite3、MySQL、Oracle supported in current.  

## Installation
```ruby
gem install rigrate
```  

if you are using oracle, you need install instant oracle clent at least.

## Basic Usage

An easy example, migrate entries hrdb users table to accounts table in oadb.

Firstly， Write the migrate script

```ruby
# file : hr_users_to_oa_accounts.rigrate

# define the data sources, use URI pattern like  DB://USR:PWD@HOST/DBNAME
ds :hr, "oracle://scott:tiger@hrdb" 
ds :oa, "mysql://root:testpwd@127.0.0.1:oadb"

# migrate snippet code
from hr.users to oa.accounts
```

Then, you can run as following:

```shell
$ rigrate execute -f hr_users_to_oa_accounts.rigrate 
```

## More Examples

1. Using native SQL. Just migrate DepNO is D00001 users to target data source.

    ```ruby
    from 
      hr.sql("select * from users where deptno = 'D00001'")
    to
      oa.accounts
    ```

2. Specify the sync condition columns. When the migration has condition columns, it will update target data instead of delete the records directly.

    ```ruby
    from 
      hr.sql("select * from users")
    to
      oa.accounts
    on :user_code => :job_code
    ```

3. Just migrate a part of entry source columns. In this situation migration with one condition is advised.

    ```ruby
    from 
      hr.users(:user_code, :name, :age, :passwd)
    to
      oa.account(:job_code, :name, :age, :password)
    on :user_code => :job_code
    ```
4. Union two data source.

    ```ruby
    from 
      hr.users union oa.accounts 
    to 
      erp.users
    ```
5. Minus two data source

    ```ruby
    from
      hr.users minus oa.accounts
    to
      erp.users
    ```

6. Join two data source. JOIN must with condition.

    ```ruby
    from
      hr.users join oa.account on :user_code => :job_code
    to
      erp.users
    ```
    
7. Data source select with after hooks

    ```ruby
    from
      hr.users(:user_code, :user_name, :password, :age) do |row|
        row[2] = "useless_password"  # make password useless
        row[3] = 16                  # make everyone 16 forever :-)
      end
    to
      oa.accounts(:job_code, :name, :pwd, :age)
    on :user_code => :job_code
    ```

## Advance Tips

1. Sync Mode  
Rigrate support support three mode, inspired by `SyncToy` a file sync tool by Microsoft. 

    **:echo** is the default mode, will delete all records of target ds which not exists in source ds  
    **:contrbiute** is same as :echo, but keep the records even it deleted in source ds  
    **:sync** mode will make two side of migration the same  
    
    use :contribute mode in migration:
    ```ruby
    from hr.users to oa.users mode :contribute
    ```

2. Using Transaction
Rigrate will do not use transaction as default. Switch it on, you can do it througt command line `--strict`.

    ```shell
      $ rigrate -f script.rigrate --strict
    ```

3. Using Rigrate in other ruby script

    ```ruby
    require 'rigrate'
    
    str =<<EOS
    ds :hr, "oracle://scott:tiger@hrdb" 
    ds :oa, "mysql://root:testpwd@127.0.0.1:oadb"
    
    from hr.users to oa.accounts
    EOS
    
    parser = Rigrate::Parser.new
    parser.lex(str).parsing
    ```
4. QuickScript. a fast way to operate data, Rigrate will load a predefined data sources file in {home}/.rigrate/ds. 
you can define some frequently used data source, and run in migration throught command line. like below:

    ```shell
    $ rigrate execute -c "from hr.users to oa.accounts"
    ```

for more usage to check `Rigrate help`

## TODO Lists

1. Integrate with schedule framework
2. Add pg/sql sever/access/csv support
3. Row data type and length checking
4. Rigrate file checking
5. More logger details

## Others

None :-)