
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
Google Analyticsは様々なログ角度が集計できますが、一部限界があって、生ログを見たい時、特にユーザのクッキーやそれに類するIDなどの粒度で取得して、レコメンドエンジン等に活用したい際に要約値しかわかないという、問題がございます。  

深い部分の仮説立案と検証は、生ログに近い方が多くの場合、データの粒度として適切で、ユーザのイベントを追加したり、粒度を変更したりした際に効率よく吸収できる方法として、データをログを保存するサーバに送りつけるという方法で、JQuery(フロントの知識が古くてすみません)などであると、このようなコードでデータを数秒ごとに送ったり、特定の動作と紐づけて動作させることで記録することができます。  
```js
$.ajax({
    type: 'POST',
    url: 'https://to.com/postHere.php', // URLはapp engine等を想定
    crossDomain: true,
    data: '{"some":"json"}', // ここにユーザデータが入るイメージ
    dataType: 'json',
    success: function(responseData, textStatus, jqXHR) {
        var value = responseData.someKey;
    },
    error: function (responseData, textStatus, errorThrown) {
        alert('POST failed.');
    }
});
```

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

## Google App EngineからPub/Subへの繋ぎ

ユーザの画面のJSから受け取ったJSONデータを一度、app engineでパースして、pub/subにpublisherで発行することができます。  

appengineに登録したコードはこのようなもを書きました。(golangとかの方が、いろいろと早いらしくいいらしいのですが、書きなれていないので、pythonのflaskを用いました)  
```python
from flask import Flask, request, jsonify
import json
from google.cloud import pubsub_v1
import google.auth
from google.oauth2 import service_account
info = json.load(open('./pubsub-publisher.json'))
credentials = service_account.Credentials.from_service_account_info(info)

app = Flask(__name__)

@app.route("/json",  methods=['GET', 'POST'])
def json_path():
        content = request.json
        print(content)

        publisher = pubsub_v1.PublisherClient(credentials=credentials)

        project_id = 'YOUR_PROJECT # project_idを入れる 
        topic_name = 'YOUR_TOPIC'  # publish先のtopicネームを入れる
        topic_path = publisher.topic_path(project_id, topic_name)

        data = json.dumps(content)
        future = publisher.publish(topic_path, data=data.encode('utf8')) # publishできるのはbytes型になる
        return f"<h1 style='color:blue'>OK</h1>"

if __name__ == '__main__':
        app.run(host='127.0.0.1', port=8080, debug=True)
```

## DataFlowのstreaming処理方法

DataFlowのstreamingは実装的には、Windowと呼ばれるstreamingの取得粒度（多くは5分などの時間間隔）を設定して、データをパイプライン処理で変換で変換し、任意の出力先に出力することが可能です。  
いろいろな用途が期待され、うまくスキャン間隔を設定することで、リアルタイムの異常検出などもできます(下図のstreaming + accumulationが該当するかと思われます)。  

<div align="center">
  <img width="650px" src="https://spotifylabscom.files.wordpress.com/2017/10/beam-model.png?w=730&zoom=2">
 <div> 図4. (spotifyのブログより) </div>
</div>

streamingのDataFlowはGCEのインスタンスが起動し、定期的に実行していることでstreamingとしているので、インスタンスが立ちっぱになるので、そこはbatch処理より安くない要因になっているように思います。  

## DataFlow pipeline  
DataFlowはpipelineで動作を定義することができ、jsonでデータが入力されているとすると、何かのサニタイズ処理、パース処理、変換処理を行うことができ、ここで、すでに分析角度が決定しているのであれば、BigQueryに投入すればいいはずです。  

```java
public class MinimalWordCount {
  public static void main(String[] args) {
    kt.funcs.testCall();
    DataflowPipelineOptions options = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class); //Dataflow.classを用いているがただのPipeLineだとローカルで動作する
    options.setProject("ai-training-16-gcp");           // GCPのプロジェクトを追加する
    options.setStagingLocation("gs://abc-tmp/STAGING"); // stagingはJavaのコンパイルされたjarファイル等が置かれる
                options.setTempLocation("gs://abc-tmp/tmp"); // 何かの中間ファイルなどが吐かれる、ことがある
                options.setRunner(DataflowRunner.class);
                options.setStreaming(true);
    options.setJobName("streamingJob6"); // Jobの名前

    Pipeline p = Pipeline.create(options);
    PCollection p1 = p.apply(PubsubIO.readStrings().fromSubscription("projects/ai-training-16-gcp/subscriptions/sub3")) // Pub/Subのデータをpull方式で取得できるサブスクリプションを追加する
        .apply(Window.<String>into(FixedWindows.of(Duration.standardMinutes(1)))); // 何分ごとにpullしてデータを処理するか
    POutput p2 = p1.apply( TextIO.write() // ここにpipelineで処理を追加すれば必要な変換やサニタイズを行うことができますが、直接GCSに吐き出しています。  
                .withWindowedWrites()
                .withNumShards(1)
                .to("gs://abc-tmp/OUTPUT") );
    p.run().waitUntilFinish();
  }
}
```


## パイプラインのSDK 1.Xからのシンタックスの変更部分
Java固定になった部分、ネームスペースが変更になった部分、型推論の部分

### 2. appengineの動作設定
refere : https://cloud.google.com/appengine/docs/flexible/python/writing-and-responding-to-pub-sub-messages

## Google App Engineで最初にデータを受け取る口を作る
  Pub/Subに投入する前に、Google App EngineでJSONデータ等を受け取る必要があります。 
  このとき、 `https://cloud.google.com/appengine/docs/standard/python3/quickstart` を参考に、簡単なアプリを開発可能です。  
  
  
