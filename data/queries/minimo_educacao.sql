-- minimo_educacao.sql
-- Placeholder {SCHEMA_ANO} substituído pelo Python (ex: mil2026).
-- A tabela saldocontabil_ex (mil2001) contém múltiplos exercícios.
-- Coluna COEXERCICIO filtra ano corrente e ano anterior (para L8/L19 — Superávit).
--
-- COLUNAS RECEITA:
--   PREVISAO ATUALIZADA : COCONTACONTABIL IN (521110000, 521210100, 521210200)
--   RECEITAS REALIZADAS : COCONTACONTABIL IN (621200000, 621300000)
--
-- COLUNAS DESPESA (MDE):
--   DOTACAO ATUALIZADA  : SUBSTR(COCONTACONTABIL,1,5) IN (52211,52212,52215,52219)
--   DESPESAS EMPENHADAS : SUBSTR(COCONTACONTABIL,1,5) IN (62213)
--   DESPESAS LIQUIDADAS : SUBSTR(COCONTACONTABIL,1,7) IN (6221303,6221304,6221307)
--   DESPESAS PAGAS      : SUBSTR(COCONTACONTABIL,1,7) IN (6221304)

SELECT
    sc.coexercicio     AS inano,
    sc.coug,
    sc.couo,
    sc.cofonte,
    fr.cofontefederal,
    sc.cocontacontabil,
    sc.cocontacorrente,
    sc.conatureza,
    sc.cofuncao,
    sc.cosubfuncao,
    sc.coprojeto,
    sc.cosubtitulo,
    sc.vadebito,
    sc.vacredito,
    sc.inmes,
    cc.incontacorrente,

    -- CO (classificado apenas para o ano corrente; ano anterior retorna NULL)
    CASE
        WHEN sc.coexercicio = TO_CHAR(EXTRACT(YEAR FROM SYSDATE))
         AND sc.couo IN (18101, 18903)
         AND sc.cofuncao     = 12
         AND SUBSTR(sc.cofonte,1,3) IN (
                 '100','101','102','104','105','109','122','702',
                 '300','301','302','304','305','309','322','802'
             )
         AND SUBSTR(sc.conatureza,5,2) NOT IN (
                 '01','03','08','18','20','31','46','48','49','59','94'
             )
         AND LPAD(sc.cofuncao,  2,'0')
          || LPAD(sc.cosubfuncao,3,'0')
          || LPAD(sc.coprograma, 4,'0')
          || LPAD(sc.coprojeto,  4,'0')
          || LPAD(sc.cosubtitulo,4,'0')
             NOT IN (
                 '12122620326190009','12122822185020037','12122822185040103',
                 '12243622191070299','12361622124460001','12361622129640001',
                 '12361622136320001','12361622140470002','12361622191070300',
                 '12362622124460002','12362622129640004','12362622136320002',
                 '12362622140470003','12364622140890004','12364622140890019',
                 '12364622190380002','12364622190380003','12364622190600003',
                 '12364622190600004','12364622190830002','12364622190830015',
                 '12364622191080003','12364622191080004','12364622191310002',
                 '12365622124420001','12365622124460004','12365622129649316',
                 '12365622129649317','12365622136320003','12365622136320005',
                 '12365622140470001','12366622124460003','12366622129649314',
                 '12366622136320006','12366622140470006','12367622124460005',
                 '12367622129649319','12367622136320004','12367622140470005',
                 '12368622191070280','28421621724260004','28421621724260087',
                 '28421621724268424','28846000190410006','28846000190410137',
                 '28846000191270072','12122622142020005','12243622191070022',
                 '12243622191070012','12361622191070050','12453622142020010',
                 '12122822185046980',
                 -- 12421621724260006: na LOA 2026 a ação 2426 migrou da função 28
                 -- (PTs 28421621724260004/0087/8424, já excluídos acima) para a
                 -- função 12/subfunção 421; a exclusão oficial do RREO acompanhou
                 -- (conferido contra o Anexo 8 do 1º bim/2026 — linha 20.8).
                 '12421621724260006'
             )
        THEN '1001'
        WHEN sc.coexercicio = TO_CHAR(EXTRACT(YEAR FROM SYSDATE))
         AND (sc.coug = 160903 OR sc.couo = 18903)
         AND sc.incategoria = 1
         AND sc.cofuncao    = 12
         AND (
                 SUBSTR(sc.cofonte,1,3) IN (
                     '100','101','102','105','109','122',
                     '300','301','302','305','309','322','702','802'
                 )
             OR  sc.cofonte IN (104100000, 104200000, 304100000, 304200000)
             )
        THEN '1070'
    END AS CO,

    -- ND
    CASE cc.incontacorrente
        WHEN '13' THEN SUBSTR(sc.cocontacorrente, 33, 6) || '00'
        WHEN '20' THEN SUBSTR(sc.cocontacorrente, 33, 8)
        WHEN '77' THEN SUBSTR(sc.cocontacorrente, 34, 6) || '00'
        WHEN '78' THEN SUBSTR(sc.cocontacorrente, 33, 8)
        WHEN '16' THEN TO_CHAR(sc.conatureza)            || '00'
    END AS ND

FROM mil2001.saldocontabil_ex  sc
LEFT JOIN {SCHEMA_ANO}.fonterecurso  fr ON sc.cofonte         = fr.cofonte
LEFT JOIN {SCHEMA_ANO}.contacontabil cc ON sc.cocontacontabil = cc.cocontacontabil

WHERE sc.coexercicio IN (
          TO_CHAR(EXTRACT(YEAR FROM SYSDATE)),
          TO_CHAR(EXTRACT(YEAR FROM SYSDATE) - 1)
      )
  AND (
          -- Receita: famílias completas 5211/5212 (previsão) e 6212/6213
          -- (realizadas), conforme a regra — inclui contas de dedução como
          -- 621390101 (restituições; ex.: -285.683,16 na linha 1.8 do 1º
          -- bim/2026, conferido contra o RREO oficial). As exceções
          -- 521120101 e 621310100 (dedução FUNDEB) são tratadas no etl.py.
          SUBSTR(sc.cocontacontabil,1,4) IN ('5211','5212','6212','6213')
       OR SUBSTR(sc.cocontacontabil,1,5) IN ('52211','52212','52215','52219')
       OR SUBSTR(sc.cocontacontabil,1,5) IN ('62213')
       -- Quadro 30 (RP) — regra atualizada em 2026-06-12 (contas exatas,
       -- sem SUBSTR; fórmula validada exatamente contra RREO oficial 1º
       -- bim/2026 — ver etl.py CC9_AC/CC9_AE/CC9_AD_EXTRA/CC9_AF e
       -- regra_minimo_educacao.txt):
       --   (ac) saldo inicial:  531100000/531200000/532100000/532200000 (inmes=0)
       --   (ae) pagos:          631400000/631820000/632210100/632210200/632210300/632210400
       --   (ad) extra (soma com ae): 631300000/631810000
       --   (af) cancelados:     631900000/632900000
       OR sc.cocontacontabil IN (531100000,531200000,532100000,532200000,
                                 631300000,631400000,631810000,631820000,
                                 631900000,632210100,632210200,632210300,632210400,
                                 632900000)
      )
