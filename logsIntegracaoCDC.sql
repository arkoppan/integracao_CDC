
--DROP TABLE [swd-bigdata].[0_PAR].[log_integracaoWebService_Optout_DBM_status]
--CREATE
-- TABLE [swd-bigdata].[0_PAR].[log_integracaoWebService_Optout_DBM_status]
--     ( id        INT  PRIMARY KEY UNIQUE NOT ENFORCED
--     , descricao VARCHAR(255) )

--DROP TABLE [swd-bigdata].[0_PAR].[log_integracaoWebService_Optout_DBM]
--CREATE
-- TABLE [swd-bigdata].[0_PAR].[log_integracaoWebService_Optout_DBM]
--     ( id             INT IDENTITY(1,1) PRIMARY KEY UNIQUE NOT ENFORCED
--	 , dataHoraInicio DATETIME
--	 , dataHoraFim    DATETIME
--	 , jsonEnviado    VARCHAR(8000) NULL
--	 , jsonRecebido   VARCHAR(8000) NULL
--	 , urlRequest     VARCHAR(8000) NULL
--	 , idStatus       INT
--	 , chaveUQ        UNIQUEIDENTIFIER
--	 , httpStatusCode INT )

--DROP TABLE [swd-bigdata].[0_PAR].[log_integracaoWebService_Optout_DBM_erro]
--CREATE
-- TABLE [swd-bigdata].[0_PAR].[log_integracaoWebService_Optout_DBM_erro]
--     ( id      INT IDENTITY(1,1) PRIMARY KEY UNIQUE NOT ENFORCED
--	 , chaveUQ UNIQUEIDENTIFIER
--	 , msg     VARCHAR(8000) )

--DROP TABLE [swd-bigdata].[0_PAR].[log_integracaoWebService_Optout_DBM_controle]
--CREATE
-- TABLE [swd-bigdata].[0_PAR].[log_integracaoWebService_Optout_DBM_controle]
--     ( id               INT IDENTITY(1,1) PRIMARY KEY UNIQUE NOT ENFORCED
--	 , num_docto        BIGINT
--	 , chaveUQ          UNIQUEIDENTIFIER
--	 , dataHoraInsercao DATETIME )

--DROP TABLE [swd-bigdata].[0_PAR].[log_integracaoWebService_Optout_DBM_parametros]
--CREATE
-- TABLE [swd-bigdata].[0_PAR].[log_integracaoWebService_Optout_DBM_parametros]
--     ( id            INT PRIMARY KEY UNIQUE NOT ENFORCED
--	 , sqlBuscaCPFs  VARCHAR(8000)
--	 , urlRequest    VARCHAR(8000)
--	 , urlLogin      VARCHAR(8000)
--	 , client_id     VARCHAR(8000)
--	 , client_secret VARCHAR(8000) )

--TRUNCATE TABLE [swd-bigdata].[0_PAR].[log_integracaoWebService_Optout_DBM_status]
--INSERT
--  INTO [swd-bigdata].[0_PAR].[log_integracaoWebService_Optout_DBM_status]
--     ( id
--	 , descricao )
--SELECT 5 , 'Sem CPFs para exportar'     UNION ALL
--SELECT 4 , 'Erro no envio'              UNION ALL
--SELECT 3 , 'Erro na execu��o do script' UNION ALL
--SELECT 2 , 'Sucesso'					  UNION ALL
--SELECT 1 , 'Em execu��o'

--TRUNCATE TABLE [swd-bigdata].[0_PAR].[log_integracaoWebService_Optout_DBM_parametros]
--INSERT
--  INTO [swd-bigdata].[0_PAR].[log_integracaoWebService_Optout_DBM_parametros]
--     ( sqlBuscaCPFs 
--     , urlRequest   
--     , urlLogin     
--     , client_id    
--     , client_secret )
--SELECT sqlBuscaCPFs =
--'SELECT TOP 10
--       cpf = dbo.fn_formataCPF(A.cpf)
--     , cpfInt = A.cpf
--  FROM [swd-bigdata].[1_RAW].[dim_optOut_DBM] A WITH(NOLOCK)
-- WHERE 1 = 1
--   AND NOT EXISTS ( SELECT TOP 1 1
--                      FROM [swd-bigdata].[0_PAR].[log_integracaoWebService_Optout_DBM_controle] X WITH(NOLOCK)
--					 WHERE X.num_docto = A.cpf )'
--,urlRequest    = 'https://apihml.portoseguro.com.br/rh/v1/atualizacao/dbm/'
--,urlLogin      = 'https://apihml.portoseguro.com.br/rh/autenticacao/v1/login'
--,client_id     = 'cea244a0-a8b9-495c-a155-2b3dacd175af'
--,client_secret = '2cc465e9-d9a0-460e-8a01-73ad3eaa8880'