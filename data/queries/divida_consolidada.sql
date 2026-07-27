-- divida_consolidada.sql — Dívida Consolidada Líquida (RGF Anexo II)
-- Regras completas em tools/regra_divida_consolidada.txt
--
-- MIL2001.SALDOCONTABIL_EX é a view histórica fixa (multi-exercício), mesmo
-- padrão de rcl.sql e restos_a_pagar.sql — não usa {SCHEMA_ANO}.
--
-- A query extrai o SUPERCONJUNTO de linhas contábeis das regras. COUG e
-- COFONTE são exportados como colunas — as exclusões/exceções por COUG e
-- COFONTE (Disponibilidade de Caixa Bruta, Depósitos Restituíveis e Valores
-- Vinculados, Demais Haveres Financeiros) são aplicadas em Python
-- (build_divida_consolidada_data), não aqui, mesmo padrão de rcl.sql/
-- resultado_primario_nominal.sql (SQL extrai o bruto, Python classifica).
--
-- RPP Inscrito (632110100,632110300) acumula o exercício inteiro (sem
-- trava de INMES=0). RPNP a Pagar usa (631300000,631810000). Ambas filtram
-- SUBSTR(CONATUREZA,2,1) NOT IN (2,6) — exclui GND 2 (Juros e Encargos da
-- Dívida) e GND 6 (Amortização da Dívida), que já estão refletidos na
-- Dívida Consolidada (I) e não devem contar de novo na dedução de RP.
--
-- Depósitos Restituíveis e Valores Vinculados: a faixa 218800000-218899999
-- é extraída inteira; os sub-ranges/contas excluídos pela regra são
-- filtrados em Python (não dá para combinar IN(...) e BETWEEN na mesma
-- lista, e as exclusões variam por sub-faixa).
--
-- max_mes_fechado — mesmo padrão de rcl.sql: max(inmes) de
-- {SCHEMA_ANO}.mesfechado. Limita o seletor de mês do dashboard aos meses
-- já fechados, para que a RCL Ajustada (que depende de mesfechado) sempre
-- tenha dado disponível para qualquer mês selecionável.

SELECT
  s.coexercicio                 AS ano,
  s.inmes,
  s.cocontacontabil,
  s.conatureza,
  s.coug,
  s.cofonte,
  s.vadebito,
  s.vacredito,
  (SELECT MAX(m.inmes)
     FROM {SCHEMA_ANO}.mesfechado m
    WHERE m.inmes BETWEEN 1 AND 12) AS max_mes_fechado
FROM mil2001.saldocontabil_ex s
WHERE s.inmes BETWEEN 0 AND 12
  AND s.coexercicio = EXTRACT(YEAR FROM SYSDATE)
  AND (
    s.cocontacontabil IN (
        -- Dívida Consolidada (I) ------------------------------------------
        -- Empréstimos Internos
        212117201, 222110200, 212115201,
        -- Empréstimos Externos
        212217201, 222210200, 222910100, 212215201,
        -- Reestruturação da Dívida de Estados e Municípios
        222130401, 212137401, 212135401,
        -- Financiamentos Internos
        222310102,
        -- Parcelamento e Renegociação — De Tributos
        214136201, 224130201,
        -- Parcelamento e Renegociação — De Contribuições Previdenciárias
        224130205, 211435102,
        -- Parcelamento e Renegociação — De Demais Contribuições Sociais
        214138202, 224130202,
        -- Precatórios Posteriores a 05/05/2000 (Vencidos e não pagos)
        221210202, 113510802,
        -- Outras Dívidas
        218910105, 218910108,
        -- Deduções (II) — Demais Haveres Financeiros ----------------------
        112410201, 112410302, 112410303, 112410304, 112910301,
        121110301, 121110302, 121110321, 121110322, 121310102,
        -- Deduções (II) — Restos a Pagar Processados (saldo a pagar) ------
        -- RPP Inscrito
        632110100, 632110300,
        -- RPNP a Pagar
        631300000, 631810000
    )
    -- Deduções (II) — Disponibilidade de Caixa Bruta (ranges)
    OR s.cocontacontabil BETWEEN 111110000 AND 111999999
    OR s.cocontacontabil BETWEEN 114110901 AND 114110999
    -- Deduções (II) — Depósitos Restituíveis e Valores Vinculados (faixa bruta)
    OR s.cocontacontabil BETWEEN 218800000 AND 218899999
  )
