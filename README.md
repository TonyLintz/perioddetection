## Period Detection ##

### Project 維護規範 ###

1. 修改程式請從master建立branch後開始修改(Develop-YourName-Purpose)
2. 請將程式放在PeakDetection目錄下，再根據module分類
3. 請將中繼資料或是輸出的資料放在Data目錄下，此資料夾下的資料原則上不放到git lab
4. 請將資料源放在git lab那台share storage
5. 程式需分成主程式負責流程(單一程式py檔)與其他功能程式(以function包裝的python程式)，切勿重頭到尾寫在一個程式裡
6. 路徑等會因不同人維護而有不同設定的變數，請透過config方式統一管理
7. 請補充該Module對應的README.md，說明程式進入點 & 執行須知

### 程式架構 ###


### 環境設定 ###

- 在專案目錄下建立虛擬環境

- submodule 設定:
    - git submodule update --init --recursive
     
    - 環境參數
        - 設定 project_share_folder，EX:project_share_folder = 'D:/OneDrive/ASUS/ShihYao Dai(戴士堯) - 新Project提案/資安異常偵測/ShareData/'
   
    - MultiCsvFileReader
        - 設定 pythonpath: "XXX/XXX/MultiCsvFileReader/Code"
    
    - 設定 Train、Test 範圍(default: Train = 2020-01-01~2020-06-30 、 Test = 2020-07-01~至今)，如欲回朔調整Test範圍請先將config中if_to_current參數調整成False，再改動test_cut_date
    
    - 設定 SourceDataType ，所要偵測的資料種類，或者下參數設定(default: 'unique_dest_cnt'，可改為total_sent_bytes)
    
### 程式執行步驟 ###
    
   - 修改設定檔: 先複製一份/PeriodDetect/config_sample.py，並改名為config.py，此檔不需被git 管理
   
   - 第一次執行: Detect_period_main.py --> train_main.py --> Predict_main.py
   
   - 執行程式可使用指令EX: python D:\GitProject\perioddetection\PeriodDetect\Detect_period_main.py --SourceData=total_sent_bytes(第一個引數為"使用的Source data") --if_to_current=True(第二個引數為"是否資料結束日期為至今) ...
   
   - 其中train_main.py這支指令會多一個if_training(是否重train)的引數，前面兩個引數如上， EX: python D:\GitProject\perioddetection\PeriodDetect\train_main.py total_sent_bytes True True
   
   - 如果已有各source ip 的model pickle檔，可直接執行Predict_main.py，並在該程式中設定是否要畫圖

### 自動化流程 ### 
    
   - 執行AutoRunning.py 這支程式即可跑完流程    