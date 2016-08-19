# 介紹

這是一個用 Python 所撰寫，可以幫助人們快速的分解 CSV 的小工具，你可以指定一個 CSV 檔案路徑以及欄位 Index，此程式會以該欄位為 Key 去分解 CSV，例如：

```
name,company
Karl,Google
Amy,Google
Gary,Facebook
Jerry,Google
Jimmy,Yahoo
Arieh,Airbnb
```

透過此程式分解後，你會得到 Google、Facebook、Airbnb、Yahoo 四個 CSV 檔案。

# 使用

python3 cutcsv.py {filepath} {index}

# 未來

- 多執行緒處理，並解決 IO 堵塞問題
    - File Lock
- 智慧設定分頁大小
- 更詳細的設定參數（分頁大小、執行緒數量、分割符號、是否有欄位名稱列、記錄檔、搬運方式資料類型...etc）
- 多檔輸入
- 進一步的效能優化，嘗試用 cython 撰寫來加速
    - 不同環境、檔案大小的效能測試報告
- 已排序資料的處理，整批搬運
- 輸出處理紀錄
- 支援 pip / apt-get 直接安裝，並同時支援 Python 2/3