def singleFieldRecord(line, ctx):
    rec = ctx.newRecord()
    rec.setRecordType(ctx.qn("{org.lilyproject}NameRecord"))
    rec.setField(ctx.qn("{org.lilyproject}Name"), line)
    ctx.writeRecord(rec)
    
def mapsNothing(line, ctx):
    pass

class MapperClass(object):
    def map(self, line, ctx):
        rec = ctx.newRecord()
        rec.setRecordType(ctx.qn("{org.lilyproject}NameRecord"))
        rec.setField(ctx.qn("{org.lilyproject}Name"), line)
        ctx.writeRecord(rec)
    
mapperInstanceMethod = MapperClass().map
