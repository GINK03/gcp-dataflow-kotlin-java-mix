
# Pub/Sub + Apache Beam（Cloud DataFlow）でログコレクターを作成する

モダンなサーバレスアーキテクチャのログ収集基盤を、GAE + Pub/Sub + DataFlow + GCS or BigQueryのコンビネーションで作成が可能です。  

また、GoogleのDataFlowのSDK version 1.XがEnd of Lifeなるということで、プログラムの移行及び、streaming処理について記述します。  

## ローカルで開発するときにの注意点
 - 1. JDKはOracle JDKの1.8を使う(OpenJDKではダメ)
 - 2. Kotlin, Scalaなどで記述することも一部できるが、Java互換の言語が型の推論に失敗する用になるので、メインの部分はJavaで記述する必要がある
  
## 全体の設計

<div align="center">
 <img width="100%" src="https://user-images.githubusercontent.com/4949982/48658826-cfa74600-ea8b-11e8-8d02-4a2e228abb61.png">
 <div> 図1. 全体のデータの流れ </div>
</div>

これは、一般的なログ収集基盤の基本的な構成になっており、最終的な出力先をBigQueryにすれば、高速なすぐ分析が開始できる基盤がサーバレスで作れますし、画像や言語のような非構造なデータであれば、CloudStrageを出力先にすることもできます。（なお、私個人の好みの問題で、いきなりSQLに投入せずに、CloudStrageに投入してあとからゆっくり、集計角度を決める方法が好みです）  

## ユーザの面からGoogle App Engineにデータを送る

## Google App EngineからPub/Subへの繋ぎ

## Pub/Subとは

Pub/Subは、細切れになりがちなデータを効率的に他のサービスにつなぐことに使えます。
<div align="center">
 <img width="600px" src="https://user-images.githubusercontent.com/4949982/47798066-f31f8080-dd6a-11e8-95b8-3bdb9aac47fc.png">
</div>
<div align="center"> 図1. </div>

データを何らかの方法で集めて、Topicとよばれる粒度で送信し、Subscriptionに連結したサービスにつなぎます。

<div align="center">
 <img width="600px" src="https://user-images.githubusercontent.com/4949982/47800032-d5541a80-dd6e-11e8-9b52-bdddda5a9e74.png">
</div>
<div align="center"> 図2. </div>


## DataFlowのstreaming処理方法
DataFlowのstreamingは実装的には、Windowと呼ばれるstreamingの取得粒度（多くは5分などの時間間隔）を設定して、データをパイプライン処理で変換で変換し、任意の出力先に出力することが可能です。  
いろいろな用途が期待され、リアルタイムの異常検出などもできます。  

DataFlowは背景にGCEのインスタンスが起動することになり、立ちっぱになるので、そこはbatch処理より安くない要因になっているように思います。  


## パイプラインのSDK 1.Xからのシンタックスの変更部分
Java固定になった部分、ネームスペースが変更になった部分、型推論の部分

### 2. appengineの動作設定
refere : https://cloud.google.com/appengine/docs/flexible/python/writing-and-responding-to-pub-sub-messages

## Google App Engineで最初にデータを受け取る口を作る
  Pub/Subに投入する前に、Google App EngineでJSONデータ等を受け取る必要があります。 
  このとき、 `https://cloud.google.com/appengine/docs/standard/python3/quickstart` を参考に、簡単なアプリを開発可能です。  
  
  
