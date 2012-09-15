#! /usr/bin/env python
import inspect
import redis

r = redis.Redis ()
r.flushall ()

class classproperty(object):
    def __init__(self, getter):
        self._getter = getter

    def __get__(self, instance, owner):
        return self._getter(owner)

class RField ():
    required = False
    default = None

    def __init__ (self, *k, **kw):
        if kw.has_key ("required"):
            self.required = kw['required']
        if kw.has_key ("default"):
            self.default = kw['default']
        

class StringField (RField):
    pass

class IntField (RField):
    pass

class ForeignKey (RField):
    def __init__ (self, *k, **kw):
        RField.__init__ (self, *k, **kw)
        self.relation = k[0]

class RModel (object):
    
    id = IntField ()

    keyvals = {}
    locals = []

    def __init__ (self, *k, **kw):
        self.newobj = True
        self.keyvals = {}
        self.locals = []
        self.reinit ()
        for i in self.locals:
            fieldobj = object.__getattribute__(self, i)
            if kw.has_key (i):
                self.keyvals[i] = kw[i]
            else:
                if fieldobj.required == True:
                    if fieldobj.default is not None:
                        self.keyvals[i] = fieldobj.default
                    else:
                        raise Exception ("Need a default value for %s" % (i))

    def from_id (self, id):
        self.seq = int(id)
        self.newobj = False
        return self
        
    def reinit (self):
        #for name, obj in inspect.getmembers (self):
        ##    if isinstance (obj, RField):
        #        self.keyvals[name] = obj.default
        inspect.getmembers (self)

    def validate (self):
        if kw.has_key (name):
            self.keyvals[name] = kw[name]
        elif obj.default is not None:
            self.keyvals[name] = obj.default
        else:
            if obj.required:
                raise AttributeError ("This field is required")

    @property
    def classkey (self):
        return 'rmodel:%s' % (self.__class__.__name__.lower ())

    def sequence (self):
        seq_av_at = "%s:__seq__" % (self.classkey)
        seq = r.incr (seq_av_at)
        return seq

    def prepare_key (self, key, for_seq):
        r_key = "%s:%d:%s" % (self.classkey, for_seq, key)
        return r_key

    def save (self):
        if self.newobj:
            using_sequence = self.sequence ()
            self.keyvals['id'] = using_sequence
            self.seq = using_sequence
        else:
            using_sequence = self.seq
        for key, val in self.keyvals.items ():
            r_key = self.prepare_key (key, using_sequence)
            r.set (r_key, val)
        self.keyvals = {}
        self.newobj = False

    @classproperty
    def objects (self):
        return InternalObjectList (self)

    def __getattribute__ (self, attr):
        attrib = object.__getattribute__(self, attr)
        if not isinstance (attrib, RField):
            return attrib
        if attr not in self.locals:
            self.locals.append (attr)
        if self.newobj:
            if self.keyvals.has_key (attr):
                return self.keyvals[attr]
            else:
                fieldobj = object.__getattribute__(self, attr)
                return fieldobj.default

        answer = r.get (self.prepare_key (attr, self.seq))
        fieldobj = object.__getattribute__(self, attr)
        if answer == None:
            answer = fieldobj.default
        else:
            if isinstance (fieldobj, ForeignKey):
                fkey = r.get (self.prepare_key ('__relationfor__', self.seq))
                cls = globals ()[fkey]
                return cls.objects.get (id = answer)

        return answer
        
    def __setattr__ (self, attr, val):
        try:
            attrib = object.__getattribute__(self, attr)
        except AttributeError:
            object.__setattr__ (self, attr, val)
            return

        if not isinstance (attrib, RField):
            object.__setattr__ (self, attr, val)
            return

        if isinstance (attrib, ForeignKey):
            self.keyvals[attr] = val.id
            self.keyvals['__relationfor__'] = attrib.relation
        else:
            self.keyvals[attr] = val


class InternalObjectList (object):
    
    def __init__ (self, classfor):
        self.classfor = classfor

    def get_by_id (self, id):
        clsfor_obj = self.classfor()
        clsfor_obj.from_id (id)
        return clsfor_obj
        return
        for name, obj in inspect.getmembers (clsfor_obj):
            if isinstance (obj, RField):
                key = clsfor_obj.prepare_key (name, int(id))

    def get (self, *k, **kw):
        if kw.has_key ('id'):
            return self.get_by_id (kw['id'])

class Profile (RModel):
    fbid = StringField ()

class User (RModel):
    username = StringField (required = True)
    first_name = StringField (required = True)
    last_name = StringField ()
    password = StringField (required = True)
    email = StringField (required = True)



"""
class FK (RModel):
    name = StringField ()

class Test (RModel):
    username = StringField ()
    password = StringField ()
    rel = ForeignKey ('FK')
    defa = StringField (default = 'a')
    req = StringField (required = True, default = 'abc')


fk = FK (name = 'abc')
fk.save ()

print "FKID:", fk.id

t = Test (username = "u", password = "p")
t.rel = fk
t.save ()
print t.id
k= t.rel
print "Naaam:", k.name


#t.username = "new"
#t.save ()

#t = Test ()
#t.username = 22
for i in r.keys ():
   print i, r.get (i)
"""
