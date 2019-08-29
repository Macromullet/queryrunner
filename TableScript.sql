USE [Test]
GO

/****** Object:  Table [dbo].[TargetTable]    Script Date: 8/29/2019 8:58:59 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[TargetTable](
	[ID] [bigint] IDENTITY(1,1) NOT NULL,
	[Column1] [nvarchar](50) NOT NULL,
	[Column2] [nvarchar](50) NOT NULL,
	[Column3] [nvarchar](50) NULL
) ON [PRIMARY]
GO


