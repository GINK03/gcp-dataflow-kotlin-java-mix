# Cloud DataFlowをKotlinで書く

以前投稿した基本時な項目に加えて、特にバッチ処理における

1. SQLでは難しいデータの集計の角度
2. 入出力にJSONを使うことでデータのユーザの独自のデータ型の定義
3. 複数のGCSのバケットを入力にする
4. DataFlowのリソース管理

という項目を追加して、より実践的な側面と使うにあたって気をつけるべきポイントを示しています  


## Javaではなく、Kotlinで書くモチベーション
Kotlinが標準で採用しているラムダ式を用いたメソッドチェーンと、Cloud DataFlow(OSSの名前はApache Beam)の作りが類似しており、ローカルでのKotlinやScalaで書いた集計コードの発想を大きく書き換えることなく、Cloud DataFlowで利用できます。　　　

また、シンタックスもKotlinはJavaに比べると整理されており、データ分析という視点において、見通しが良く、最近はAndroidの開発や、サーバサイドの開発だけでなく、データサイエンスにも転用が可能となって来ております  


## Cloud DataFlowとは

GCPで提供されているクラウド集計プラットフォームで、Apache Beamという名前でOSSで公開されています。  

Map Reduceの発展系のような印象を受ける作りになっており、何段階にもパイプラインを結合して様々な処理ができるようになっています  

ストリーミング処理も得意なことがメリットとしてあげられていますが、バッチ処理も強力です  

また、専用にあらかじめインスタンスを確保しておく必要がないため、サーバレスのビッグデータ環境のようにも扱えます（CPUやDISKの制約はGCPのComputing Engineと共用なようです）

- Googleが考える、データの取り扱い
<p align="center">
  <img width="500px" src="https://user-images.githubusercontent.com/4949982/27850699-50d8ec72-6191-11e7-88e6-ef28e3f35496.png">
</p>

- この処理に則って、様々な分析プラットフォームのクラウドサービスが展開されている  

<p align="center">
  <img width="500px" src="https://user-images.githubusercontent.com/4949982/27850736-8beeba58-6191-11e7-8f08-88940c3ecfb7.png">
</p>

- AmazonのElastic Map Reduceと競合する製品だと思われますが、サーバの台数に制限がないことと、自動リソース管理、Map Reduceの操作が二段階ではなく、任意の回数行うことができます

## AWS Elastic Map Reduceとの違い
<p align="center">
  <img width="500px" src="https://user-images.githubusercontent.com/4949982/27850775-ba6ac0b6-6191-11e7-8829-f980397d3aad.png">
</p>
<div align="center">elastic map reduceイメージ</div>

<p align="center">
  <img width="500px" src="https://user-images.githubusercontent.com/4949982/27850831-07243662-6192-11e7-808a-f1f8dda4957c.png">
</p>
<div align="center">Google Cloud DataFlow</div>

Amazon Elastic Map Reduceに比べて、多段にした処理を行うことができることが、最大のメリットだと感じます（複雑な集計が一気通貫でできる)

## Google Cloud DataFlow特徴

- JVMを基準としたプログラミング言語で分析・集計処理を書けるので、非構造化データに対応しやすい  
- GCPのDataStorage（AWSのS3のようなもの）に保存するのでコストが安い  
- Apache Sparkなどのラムダ式を用いたデータ構造の変換と操作が類似しており、データ集計に関する知識が利用できる  
- SQLでないので、プログラムをしらないと集計できない(デメリット)

## Requirements
- maven( 3.3.9 >= )
- Kotlin( 1.1 >= )
- Oracle JDK( 1.8 >= )
- Google Cloud SDK( 158.0.0 >= ) 

## Google Cloud SDKのインストールとセットアップ  
ローカルのLinuxマシンからGCPに命令を送るのに、Google Cloud SDKのgcloudというツールをインストールしておく必要があります  

この例ではLinuxを対象としています  
```console
$ wget https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-158.0.0-linux-x86_64.tar.gz
$ tar zxvf google-cloud-sdk-158.0.0-linux-x86_64.tar.gz
$ ./google-cloud-sdk/install.sh 
Welcome to the Google Cloud SDK!
...
Do you want to help improve the Google Cloud SDK (Y/n)? <<- Yと入力
Do you want to continue (Y/n)?  <<- Yと入力
```

bashrcのリロード
```console
$ source ~/.bashrc
```

gcloud initして、gcloudの認証を通します
```console
$ gcloud init # gcloundのセットアップ
 [1] alicedatafoundation
 [2] machine-learning-173502
 [3] Create a new project
 Please enter numeric choice or text value (must exactly match list 
item): 2 # 使っているプロジェクトを選択
```
asia-northeast1-[a|b|c]のリージョンを設定  
```console
If you do not specify a zone via a command line flag while working 
with Compute Engine resources, the default is assumed.
 [1] asia-east1-c
 [2] asia-east1-b
 [3] asia-east1-a
 ...
```

クレデンシャル（秘密鍵などが記述されたjsonファイル）の環境設定の発行と設定  
Google Apiで発行してもらう  
<div align="center">
  <img width="450px" src="https://user-images.githubusercontent.com/4949982/29770222-db334400-8c28-11e7-8187-3584dff025b7.png"> 
</div>

クレデンシャルを環境変数に通します  

必要ならば、bashrcに追加しておくと、ログイン時に自動でロードされます  
(ターミナルでの利用を前提としています)  

```cosnole
$ export GOOGLE_APPLICATION_CREDENTIALS=$HOME/gcp.json
```

## GCP側のセットアップ操作
### 1. Projectの作成
<p align="center">
  <img width="500px" src="https://user-images.githubusercontent.com/4949982/27850990-faa3ea94-6192-11e7-9032-eb9c94c1802a.png">
</p>
<div align="center">1. Projectの作成</div>

### 2. CloudStrageの作成
<p align="center">
  <img width="500px" src="https://user-images.githubusercontent.com/4949982/27851018-2e9c20a0-6193-11e7-91ee-a4982f8490e1.png">
</p>
<div align="center">2. CloudStrageの作成</div>

### 3. Keyの作成
<p align="center">
  <img width="500px" src="https://user-images.githubusercontent.com/4949982/27851096-aab21550-6193-11e7-97b0-4eef482d64cd.png">
</p>
<div align="center">3. Keyの作成</div>
(ここで作成したjsonファイルはダウンロードして環境変数にセットしておく必要があります)

### 4. Codeを書く
<p align="center">
  <img width="500px" src="https://user-images.githubusercontent.com/4949982/27851130-d8999a2e-6193-11e7-8b4c-f9fe3d5144e1.png">
</p>
<div align="center">4. Codeを書く</div>
任意の集計処理を記述します

## Kotlinで記述されたサンプルのコンパイル＆実行
私が作成したKotlinのサンプルでの実行例です。  
シェイクスピアの小説などの文章から、何の文字が何回出現したかをカウントするプログラムです  
git clone(ダウンロード)
```console
$ git clone https://github.com/GINK03/gcp-dataflow-kotlin-java-mix
```

コンパイル　
```console
$ mvn package
```
クリーン（明示的にtargetなどのバイナリを消したい場合）
```console
$ mvn clean
```
GCPに接続して実行
```console
$ mvn exec:java
```

これを実行すると、ご自身のGCPのDataStrageに結果が出力されているのを確認できるかと思います

## KotlinのDataFlowのプログラムの説明
多くの例では、Word(単語)カウントを行なっていますが、今回の例では、Char(文字)のカウントを行います  

Googleが無償で公開しているシェイクスピアのテキストを全て文字に分解して、group byを行いどの文字が何回出現しているのか、カウントします  

このプログラムを処理ブロック（一つのブロックはクラスの粒度で定義されている）で図示すると、このようになります  
<p align="center">
  <img width="550px" src="https://user-images.githubusercontent.com/4949982/30238782-fc85323a-9588-11e7-9457-a7ca434c662f.png">
</p>

各クラスの定義はこのように行いました  

#### KotlinProc1クラス
```kotlin
public class KotlinProc1 : DoFn<String, String>() {
  override fun processElement(c : DoFn<String,String>.ProcessContext) {
    val elem = c.element()
    elem.toList().map { 
      val char = it.toString()
      c.output(char)
    }
  }
}
```
#### KotlinProc2クラス
```kotlin
public class KotlinProc2 : DoFn<String, KV<String, String>>() {
  override fun processElement(c : DoFn<String, KV<String,String>>.ProcessContext) {
    val char = c.element()
    c.output(KV.of(char, "1"))
  }
}
```
#### GroupByKey
```kotlin
 GroupByKey.create<String,String>() 
```
#### KotlinProc3クラス
```kotlin
public class KotlinProc3 : DoFn<KV<String, Iterable<String>>, String>() {
  override fun processElement(c : DoFn<KV<String, Iterable<String>>, String>.ProcessContext) {
    val key = c.element().getKey()
    val iter = c.element().getValue()
    val list = mutableListOf<String>()
    iter.forEach {  list.add(it.toString()) }
    c.output(key.toString() + ": " + list.size.toString()); 
  }
}
```

### GCP DataFlowを用いず生のKotlinを用いて同等の処理を書く
類似していることが確認できます  
```kotlin
import java.io.*
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.io.BufferedReader
import java.io.InputStream
import java.io.File
import java.util.stream.Collectors
import java.util.stream.*
fun main( args : Array<String> ) {
  val dataframe = mutableListOf<String>() 
  Files.newDirectoryStream(Paths.get("contents/"), "*").map { name -> //データをオンメモリにロード
    val inputStream = File(name.toString()).inputStream()
    inputStream.bufferedReader().useLines { xs -> xs.forEach { dataframe.add(it)} }
  }

  dataframe.map {
    it.toList().map { // DataFlowのKotlinProc1に該当
      it.toString()
    }
  }.flatten().map { // DataFlowのKotlinProc2に該当
    Pair(it, "1")
  }.groupBy { // DataFlowのGroupByKeyに該当
    it.first
  }.toList().map { // DataFlowのKotlinProc3に該当
    val (key, arr) = it
    println("$key : ${arr.size}")
  }
}
```

### WordCountレベルの、ここまでのまとめ
ローカルで分析すると一台のマシンで収まるメモリの量しか取り扱うことができないので、ビッグデータになると、必然的にスケーラブルなCloud DataFlowのようなサービスを利用する必要があります。  
このように、ローカルでの分析の方法とビッグデータの分析の方法が似ていると、発想を切り替える必要がなかったり、一人でスモールなデータからビッグデータまで低いコストで分析できるので、生産性を高めることができます。

# ビッグデータでSQLの代わりにGoogle DataFlowを使うプラクティス
通常使うぶんにはSQLのインターフェースが用意されていることが多いですが、SQL以外で分析したい、SQLでは分析しにくい、などのモチベーションがる場合、Google DataStrageにコピーすることでも分析します。  

[minioのmc](https://github.com/minio/mc)というコマンドでs3 -> gcsの同期を簡単に実行できます
```console
$ mc mirror s3/any-s3-bucket/ gcs/any-gcs-bucket/
```

### 特定のキーでデータを直列化する  
SQLでも無理くり頑張ればできないこともないのですが、かなりアドホックなのと、ビッグデータに全適応しようとする場合、かなり困難が伴います。  

SQLがユーザ行動のような人によって可変長なテーブルの上に関連データベースとして表現しにくいからなのですが、無理にそのようにせず、KVSやDocument志向の発想を持ち込んで、特定のキーで転地インデックスを作成することが可能になります。

参考：[SQLで転置インデックス](https://qiita.com/k24d/items/79bc4828c918dfeeac34)

# JSONを用いたデータ構造と変形  
例えば、TreasureDataのダンプ情報は行志向でjsonで一行が表現されています。また、gzで圧縮されており、chunkingと呼ばれる適度なファイルサイズに断片化されています  

そのため、Google DataFlowで処理するときは、jsonパーサが必要です  

jsonのパースにはGsonが便利であり、型が不明なときはKotlinはAny型で受け取れるので、適切にリフレクションを用いれてば、複雑なデータ構造であっても、DataFlowの各ステップにデータを受け渡すことができます  

<p align="center"> 
  <img width="750px" src="https://user-images.githubusercontent.com/4949982/33803552-62a085e8-ddd6-11e7-9380-daea79ef6588.png">
</p>

<div align="center"> こんな感じで処理すると便利 </div>

JSONでシリアライズしたデータ構造などで統一することで、ユーザ定義型から解放されて、一応の汎用性を持つことが可能になります  

また、特定のサイズまでシュリンクしたのち、ローカルマシンで、Pythonなどでjsonを読み取ることにより、最終的なデータの加工と機械学習が容易になります  

### 具体例
```kotlin
public class KotlinProc1 : DoFn<String, String>() {
  // DoFnの定義がinput: String -> output: Stringとすることができる
  override fun processElement(c : DoFn<String,String>.ProcessContext) {
    // ここだけ、Kotlinだと切り出して、手元でコマンドラインでパイプ操作で再現することが楽なので、テストしながら開発できる
    val gson = Gson()                              
    val type = object : TypeToken<Map<String, Any>>() {}.type 

    val elem = c.element()
    val recover:Map<String,Any> = gson.fromJson<Map<String, Any>>(elem, type)                      
    if( recover.get("gender_age") == null )      
      return
    if( recover.get("os") == null)               
      return
    if( recover.get("uuid") == null || recover.get("uuid")!! == "null")
      return
    val gender_age = (recover["gender_age"]!! as Double).toInt() // <- データ中でデータの型がが判明してるならば、as Typeで変換できる                          
    val os = recover["os"]!! as String           
    val uuid = recover["uuid"] as String  
    val urlreq = try {                           
      "keyword=(.*?)&".toRegex().find( URLDec.decode( URLDec.decode( recover["request_uri"]!! as String, "UTF-8" ), "UTF-8" ) )?.groups?.get(1)?.value ?: null  // <- このように複雑なテーブルの中のデータを受け取ることができる                                    
                  
    } catch( ex : Exception ) { null }           
    if( urlreq == null || urlreq == "" )         
      return  
    // 出力の段階でここをjsonで出すようにすると、outputがList<Any>をシリアライズした、Stringに限定できるので、IFの定義が楽
    val output = gson.toJson( listOf(gender_age, os, uuid, urlreq) )
    c.output(output)
  }
}

```

# 複数のデータソースの利用
GCPの複数のDataStorageのファイルを入力し、特定のキーで結合したいなどの場合があるかと思います。   

複数のインプットを同時に入力する方法が見つからず、公式ドキュメントをかなり漁りましたが、見つからず難儀していました。  

DataFlowのSDK1.X系では、パイプラインを結合して、任意の処理にするという発想なので、inputのパイプラインを二種類以上用意して、Flattenして結合するという発想になるようです。  

```kotlin
fun runner(options: DataflowPipelineOptions, input:String, output: String) {
  val p:Pipeline = Pipeline.create(options)
  val events1:PCollection<String> = p.apply(TextIO.Read.from("gs://input1/*"));
  val events2:PCollection<String> = p.apply(TextIO.Read.from("gs://input2/*"));
  val eventsList = PCollectionList.of(events1).and(events2)
  val events = eventsList.apply(Flatten.pCollections())
  
  events
    .apply( ParDo.named("ExtractMap1").of( KotlinProc1() ) )
    .apply( ParDo.named("MakeTransit").of( KotlinProc2() ) )
    .apply( GroupByKey.create<String,String>() )
    .apply( ParDo.named("FormatResults").of( KotlinProc3() ) )
    .apply( TextIO.Write.to(output) )
  p.run()
}
```
DataFlowの管理画面ではこのように見ることができます
<div align="center">
  <img width="650px" src="https://user-images.githubusercontent.com/4949982/34406878-b970d810-ebfe-11e7-84ce-efae8bcffc7c.png">
</div>

# コンピュータリソースが必要な箇所
HadoopにおけるMapの処理の際は弱いCPUをいくつも並列化することで、データの変換を行うことができますが、Reduceの処理につなぐ時に、特定のキーでshuffle & sortが必要になります。  

この操作がメモリとディスクを大量に消費して、場合によってはコンピュータのディスクやメモリを増やす必要が出てきます。  

この制約は、GCP Cloud DataFlowにもあって、謎のUnknownエラーで落ちらた、リソース不足を疑うと良いかもしれません（Unknownのせいで48時間程度溶かしました...）  

DataFlowでは、GroupByKeyでコンピュータリソースを大量に消費するので、この前後のパイプラインで落ちていたら、ヒントになりえます。 

<div align="center">
  <img width="300px" src="https://user-images.githubusercontent.com/4949982/34407237-ad4986a2-ec00-11e7-94e3-90e0784fdc74.png">
</div>
<div align="center"> リソース不足の例、GroupByKeyのステップがエラーになります... </div>

このようなエラーが出た際には、以下の対応が有効でした
1. マシンのメモリを増やす
2. 動作させるワーカーの数を増加させる
3. 一台当たりのディスクサイズを増やす

これは、pipelineを構築する際のconfigで設定できます  
```kotlin
fun main( args : Array<String> ) {
  val options = JavaDataFlowUtils.getOptionInstance()
  // define project name
  options.setProject("machine-learning-173502")
  // define max workers (max_workerを増加させます、並列で動作させるマシンの台数の最大値です)
  options.setMaxNumWorkers(128)
  // disk size(マシン一台当たりのディスクサイズ数です、GBが単位です)
  options.setDiskSizeGb(1024*2)
  // machine type(インスタンスのタイプです、メモリ優先タイプを選択しています)
  options.setWorkerMachineType("n1-highmem-4")
  // define staging directory
  options.setStagingLocation( "gs://dataflow-stagings-machine-learning/stating-36" )
  // args order, 1st -> options, 2nd -> input data bucket, 3rd -> output data bucket
  runner(options, "gs://treasuredata-dump/20171221-json/export.*",
                  "gs://dataflow-output-machine-learning/keyword_uuid_timeseries-categories-17/*" )
}
```

# コード
[https://github.com/GINK03/gcp-dataflow-kotlin-java-mix]

# まとめ
Cloud DataFlowはサーバを自社に持つことなく、ビッグデータの分析を行うことができる素晴らしい基盤です。  

AWS EMRと比較しても、速度の面において2倍ぐらい早く感じるのと、インスタンスを事前に予約する必要がなく、立ち上がりも早いです  

今回はDataStorageに溜まったデータを一気に分析する、バッチ処理を行いましたが、AWS EMR, AWS Athena, AWS RedShift, Apache Spark, Apache Hadoop, GCP BigQueryなども使いましたが、柔軟性と速度の両立という視点では一番優れているように思います。すごい  





