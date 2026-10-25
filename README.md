# grill-backup

grill(앱 공유 플랫폼)의 모델 백업 작업. 앱과 모델 탭이 쓰는 모델 파일을 원래 주소(Civitai, Hugging Face 등)에서 받아
크기·sha256을 확인한 것만 Cloudflare R2(`models/<sha256>`)에 올린다. 디스크를 거치지 않고 여러 연결로 받으면서 바로 올린다.

- `backup.sh`: 로비에서 할 일을 받고(`/api/backups/work`), 결과를 알린다(`/api/backups/report`). 올린 파일 확인·해시 재기·지우기도 한다.
- `stream.py`: 파일 하나를 구간으로 나눠 여러 연결로 받으며 R2 나눠 올리기(multipart)로 올린다.
- `.github/workflows/model-backup.yml`: 로비가 깨우거나 하루 한 번 돈다. 러너 수는 로비가 정한다(`/api/backups/plan`).

## 설정

Secrets: `GRILL_BACKUP_SECRET` (로비의 같은 이름 환경변수와 같은 값), `R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`
(그 버킷만 읽고 쓰는 R2 API 토큰), `CIVITAI_API_KEY`.
Variables: `GRILL_LOBBY_URL`, `R2_BUCKET`.

로그는 공개된다: 파일 이름·주소·오류 원문은 남기지 않고 sha256 앞 8자리와 개수만 남긴다. 실패 이유는 로비로만 보낸다.

## 시험

```
pip install boto3 && python3 -m unittest discover -s tests
```
