
# Pub/Sub + Apache Beam（Cloud DataFlow）でログコレクターの例

モダンなサーバレスアーキテクチャのログ収集基盤である、Pub/SubとDataFlowのコンビネーションなのです。  

GoogleのDataFlowのSDK version 1.XがEnd of Lifeなるということで、プログラムの移行及び、streaming処理について記述します。  

## ローカルで開発するときにの注意点
 - 1. JDKはOracle JDKの1.8を使う
 - 2. Java互換の言語が型の推論に失敗する用になるので、一部Javaで記述する必要がある
  
## Pub/Subとは

Pub/Subは、細切れになりがちなデータを効率的に他のサービスにつなぐことに使えます。
<div align="center">
 <img width="600px" src="https://user-images.githubusercontent.com/4949982/47798066-f31f8080-dd6a-11e8-95b8-3bdb9aac47fc.png">
</div>
<div align="center"> 図1. </div>

データを何らかの方法で集めて、Topicとよばれる粒度で送信し、Subscriptionに連結したサービスにつなぎます。  

これは、一般的なログ収集基盤の基本的な構成になっており、最終的な出力先をBigQueryにすれば、高速な分析基盤がサーバレスで作れますし、画像や言語のような非構造なデータであれば、CloudStrageを出力先にすることもできます。 

## DataFlowのstreaming処理方法
windowで定期的にスキャンしている

## パイプラインのSDK 1.Xからのシンタックスの変更部分
Java固定になった部分、ネームスペースが変更になった部分、型推論の部分

### 2. appengineの動作設定
refere : https://cloud.google.com/appengine/docs/flexible/python/writing-and-responding-to-pub-sub-messages

