# Model Lab 학습 데이터

이 폴더의 JSONL만 Model Lab 학습 설정에서 선택할 수 있습니다. Golden QA 개발/평가 자료를
자동으로 복사하지 않습니다. 실제 데이터는 학습 목적에 맞게 검토하고 train/validation을
`family_id` 단위로 분리하세요. 학습/검증 사이 동일 내용도 거부합니다.

생성 모델 SFT의 한 행:

```json
{"split":"train","family_id":"unique-family","messages":[{"role":"user","content":"학습 질문"},{"role":"assistant","content":"검토된 답변"}]}
```

임베딩 contrastive의 한 행:

```json
{"split":"train","family_id":"unique-family","anchor":"검색 질문","positive":"관련 문서","negative":"어려운 오답 문서"}
```

검증 파일에는 `split: "validation"`과 다른 family_id를 사용합니다. 최종 평가 세트는
이 폴더에 넣지 않습니다. UI 검증 한도는 파일당 20MB입니다. 위 예시는 계약 설명용이며
학습 가능한 실데이터가 아닙니다.
