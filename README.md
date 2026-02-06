### requirements.txt 설치 필요!!

# 데이터 추가


fetch_new_prices.py

```jsx
YDAY=$(python -c "import pandas as pd; print((pd.Timestamp.utcnow().normalize()-pd.Timedelta(days=1)).strftime('%Y-%m-%d'))")

python fetch_new_prices.py \
  --data_dir ./data \
  --target corn \
  --start 2025-11-01 \
  --end "$YDAY" \
  --n_bars 5000 \
  --add_ema \
  --ema_span 200
```

실행시키면 2025년11월1일부터 어제 자정까지의 가격데이터 들어가게됨

만약 start를 `"$YDAY"` 로 바꾸면 어제 하루 만의 가격데이터를 수집

이제 매일매일 업데이트 할떄에는 utc 시간 기준 12시가 지났을때 start를 `"$YDAY"` 로 바꿔서 실행


# 뉴스데이터 추가

```bash
bash run_range_final.sh --slot A --start B --end C
```
A : (day or night)

B : (시작날짜)

C : (종료날짜)

시간은 UTC 기준!

예시1) 
```bash
bash run_range_final.sh --slot night --start 2026-02-05 --end 2026-02-05 
```
=> 2026년 2월 5일 오전9시부터 2월 5일 오후9시까지의 데이터 수집

예시2) 
```bash
bash run_range_final.sh --slot day --start 2026-02-01 --end 2026-02-01 
```
=> 2026년 1월 31일 오후9시부터 2월1일 오전9시까지의 데이터 수집


