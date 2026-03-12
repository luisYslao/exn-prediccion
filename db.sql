USE [Comercios]
GO

/****** Object:  Table [dbo].[comercios]    Script Date: 3/12/2026 12:18:38 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[comercios](
	[COD  COMERCIO] [varchar](255) NULL,
	[NOMB  COMERCIAL] [varchar](255) NULL,
	[MON] [float] NULL,
	[RAZON SOCIAL] [varchar](255) NULL,
	[MCC] [float] NULL,
	[RUC] [varchar](50) NULL,
	[SEGM] [float] NULL,
	[DESC  MCC] [varchar](255) NULL,
	[BCO ADQ EN] [varchar](16) NULL,
	[COD  INT AMEX] [varchar](16) NULL,
	[COD  VISANET] [varchar](16) NULL,
	[COM  CREDITO] [float] NULL,
	[COM  FLAT] [float] NULL,
	[COM  FORANEA] [float] NULL,
	[CONTRATO PMP] [varchar](255) NULL,
	[CTA ADQ EN] [varchar](255) NULL,
	[CTA ESPECIAL EN] [varchar](255) NULL,
	[DESC  SEGMENTO] [varchar](255) NULL,
	[DESC SITU ENET] [varchar](255) NULL,
	[DIR  COMERCIAL] [varchar](255) NULL,
	[DPTO  COMERCIAL] [varchar](255) NULL,
	[EJECUTIVO EN] [varchar](255) NULL,
	[ESQUEMA] [varchar](255) NULL,
	[ESQUEMA2] [varchar](255) NULL,
	[ESQUEMA3] [varchar](255) NULL,
	[ESTADO PROCESOS] [varchar](255) NULL,
	[FEC INGRESO] [varchar](255) NULL,
	[HORA INGRESO] [varchar](255) NULL,
	[MAIL  COM 1] [varchar](255) NULL,
	[MAIL ADM] [varchar](255) NULL,
	[MAIL_ADM] [varchar](255) NULL,
	[MAIL COM 2] [varchar](255) NULL,
	[NOMB BCO ADQ] [varchar](255) NULL,
	[NOMB BCO ESP] [varchar](255) NULL,
	[PAGO A TERCERO] [varchar](5000) NULL,
	[REP LEG NOM 1] [varchar](255) NULL,
	[REP LEG NOM 2] [varchar](255) NULL,
	[RRLL MAIL] [varchar](255) NULL,
	[RUC DE TERCERO] [varchar](max) NULL,
	[S N PORTE] [varchar](255) NULL,
	[SITU ENET] [varchar](255) NULL,
	[TIPO CTA ESP EN] [varchar](255) NULL,
	[UBIGEO COMER] [varchar](255) NULL,
	[UBIGEO FACT] [varchar](255) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO


