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


USE [Test]
GO

/****** Object:  StoredProcedure [dbo].[AppendTargetTable]    Script Date: 8/30/2019 3:20:26 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[AppendTargetTable]
	@Column1 NVARCHAR(50),
	@Column2 NVARCHAR(50),
	@Column3 NVARCHAR(50)
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

    INSERT INTO TargetTable(Column1, Column2, Column3) VALUES (@Column1, @Column2, @Column3)
END
GO

CREATE TYPE TargetTableTvp AS TABLE
(
	Column1 NVARCHAR(50) NOT NULL,
	Column2 NVARCHAR(50) NOT NULL,
	Column3 NVARCHAR(50) NOT NULL
)
GO

CREATE PROCEDURE [dbo].[AppendTargetTableWithTvp]
	@Data TargetTableTvp READONLY
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

    INSERT INTO TargetTable(Column1, Column2, Column3)
	SELECT * FROM @Data
END
GO


