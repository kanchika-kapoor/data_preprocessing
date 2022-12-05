[![Colab-Url][Colab-Badge]][Colab-Url]


[Colab-Badge]: https://colab.research.google.com/assets/colab-badge.svg
[Colab-Url]: https://colab.research.google.com/drive/1gdmtz4r5WuluAC3S8SC9zC0Lm-tiiGtj?usp=sharing

## Code for B9AI108 (B9AI108_2223_TMD1S) CA 2

```
Name: Kanchika Sudhirkumar Kapoor
Email_Id: 10621287@mydbs.ie
Github Url: https://github.com/kanchika-kapoor/data_preprocessing
```

### Documentation and code implementation link: 
## [Colab Notebook Link](https://colab.research.google.com/drive/1gdmtz4r5WuluAC3S8SC9zC0Lm-tiiGtj?usp=sharing)

>*Note:*
>
>*This repository contains the same methods used in colab but in a structured format*

## Data Source:
* The data is scrapped from [YCharts](https://ycharts.com/events/calendar/#/?date=2022-11-29&pageNum=1&eventGroups=earnings,dividends,splits_spinoffs,other&securitylistName=all_stocks&securityGroup=company&viewMode=week)
* The site uses one url which returns data based on various filters and the data is paginated

## Data Pipeline:
* ### Data Fetching:
    * Python Requests library is used for getting the data from the url.
* ### Data Transformation:
    * Pandas library used for creating the dataframe from the url's json response
* ### Data Storage:
    * Pypyodbc is used to connect to database on hosted azure virtual machine

### Output:

![code implementation on vm](screenshot.png "repo implemented on machine")